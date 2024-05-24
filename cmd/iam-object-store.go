// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/puzpuzpuz/xsync/v3"
)

// IAMObjectStore implements IAMStorageAPI
type IAMObjectStore struct {
	// Protect access to storage within the current server.
	sync.RWMutex

	*iamCache

	cachedIAMListing atomic.Value

	usersSysType UsersSysType

	objAPI ObjectLayer
}

func newIAMObjectStore(objAPI ObjectLayer, usersSysType UsersSysType) *IAMObjectStore {
	return &IAMObjectStore{
		iamCache:     newIamCache(),
		objAPI:       objAPI,
		usersSysType: usersSysType,
	}
}

func (iamOS *IAMObjectStore) rlock() *iamCache {
	iamOS.RLock()
	return iamOS.iamCache
}

func (iamOS *IAMObjectStore) runlock() {
	iamOS.RUnlock()
}

func (iamOS *IAMObjectStore) lock() *iamCache {
	iamOS.Lock()
	return iamOS.iamCache
}

func (iamOS *IAMObjectStore) unlock() {
	iamOS.Unlock()
}

func (iamOS *IAMObjectStore) getUsersSysType() UsersSysType {
	return iamOS.usersSysType
}

func (iamOS *IAMObjectStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err != nil {
			return err
		}
	}
	return saveConfig(ctx, iamOS.objAPI, objPath, data)
}

func decryptData(data []byte, objPath string) ([]byte, error) {
	if utf8.Valid(data) {
		return data, nil
	}

	pdata, err := madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
	if err == nil {
		return pdata, nil
	}
	if GlobalKMS != nil {
		pdata, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err == nil {
			return pdata, nil
		}
		pdata, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: objPath,
		})
		if err == nil {
			return pdata, nil
		}
	}
	return nil, err
}

func (iamOS *IAMObjectStore) loadIAMConfigBytesWithMetadata(ctx context.Context, objPath string) ([]byte, ObjectInfo, error) {
	data, meta, err := readConfigWithMetadata(ctx, iamOS.objAPI, objPath, ObjectOptions{})
	if err != nil {
		return nil, meta, err
	}
	data, err = decryptData(data, objPath)
	if err != nil {
		return nil, meta, err
	}
	return data, meta, nil
}

func (iamOS *IAMObjectStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
	data, _, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, objPath)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, item)
}

func (iamOS *IAMObjectStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteConfig(ctx, iamOS.objAPI, path)
}

func (iamOS *IAMObjectStore) loadPolicyDocWithRetry(ctx context.Context, policy string, m map[string]PolicyDoc, retries int) error {
	for {
	retry:
		data, objInfo, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
		if err != nil {
			if err == errConfigNotFound {
				return errNoSuchPolicy
			}
			retries--
			if retries <= 0 {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			goto retry
		}

		var p PolicyDoc
		err = p.parseJSON(data)
		if err != nil {
			return err
		}

		if p.Version == 0 {
			// This means that policy was in the old version (without any
			// timestamp info). We fetch the mod time of the file and save
			// that as create and update date.
			p.CreateDate = objInfo.ModTime
			p.UpdateDate = objInfo.ModTime
		}

		m[policy] = p
		return nil
	}
}

func (iamOS *IAMObjectStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, objInfo, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	if p.Version == 0 {
		// This means that policy was in the old version (without any
		// timestamp info). We fetch the mod time of the file and save
		// that as create and update date.
		p.CreateDate = objInfo.ModTime
		p.UpdateDate = objInfo.ModTime
	}

	m[policy] = p
	return nil
}

func (iamOS *IAMObjectStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPoliciesPrefix) {
		if item.Err != nil {
			return item.Err
		}

		policyName := path.Dir(item.Item)
		if err := iamOS.loadPolicyDoc(ctx, policyName, m); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error {
	var u UserIdentity
	err := iamOS.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOS.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
		iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
		return nil
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}

	if u.Credentials.SessionToken != "" {
		jwtClaims, err := extractJWTClaims(u)
		if err != nil {
			if u.Credentials.IsTemp() {
				// We should delete such that the client can re-request
				// for the expiring credentials.
				iamOS.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
				iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
				return nil
			}
			return err

		}
		u.Credentials.Claims = jwtClaims.Map()
	}

	if u.Credentials.Description == "" {
		u.Credentials.Description = u.Credentials.Comment
	}

	m[user] = u
	return nil
}

func (iamOS *IAMObjectStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]UserIdentity) error {
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePrefix) {
		if item.Err != nil {
			return item.Err
		}

		userName := path.Dir(item.Item)
		if err := iamOS.loadUser(ctx, userName, userType, m); err != nil && err != errNoSuchUser {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var g GroupInfo
	err := iamOS.loadIAMConfig(ctx, &g, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = g
	return nil
}

func (iamOS *IAMObjectStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigGroupsPrefix) {
		if item.Err != nil {
			return item.Err
		}

		group := path.Dir(item.Item)
		if err := iamOS.loadGroup(ctx, group, m); err != nil && err != errNoSuchGroup {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicyWithRetry(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy], retries int) error {
	for {
	retry:
		var p MappedPolicy
		err := iamOS.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
		if err != nil {
			if err == errConfigNotFound {
				return errNoSuchPolicy
			}
			retries--
			if retries <= 0 {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			goto retry
		}

		m.Store(name, p)
		return nil
	}
}

func (iamOS *IAMObjectStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
	var p MappedPolicy
	err := iamOS.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	m.Store(name, p)
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
	var basePath string
	if isGroup {
		basePath = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePath = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePath = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePath = iamConfigPolicyDBUsersPrefix
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePath) {
		if item.Err != nil {
			return item.Err
		}

		policyFile := item.Item
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		if err := iamOS.loadMappedPolicy(ctx, userOrGroupName, userType, isGroup, m); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

var (
	usersListKey            = "users/"
	svcAccListKey           = "service-accounts/"
	groupsListKey           = "groups/"
	policiesListKey         = "policies/"
	stsListKey              = "sts/"
	policyDBPrefix          = "policydb/"
	policyDBUsersListKey    = "policydb/users/"
	policyDBSTSUsersListKey = "policydb/sts-users/"
	policyDBGroupsListKey   = "policydb/groups/"
)

// splitPath splits a path into a top-level directory and a child item. The
// parent directory retains the trailing slash.
func splitPath(s string, lastIndex bool) (string, string) {
	var i int
	if lastIndex {
		i = strings.LastIndex(s, "/")
	} else {
		i = strings.Index(s, "/")
	}
	if i == -1 {
		return s, ""
	}
	// Include the trailing slash in the parent directory.
	return s[:i+1], s[i+1:]
}

func (iamOS *IAMObjectStore) listAllIAMConfigItems(ctx context.Context) (map[string][]string, error) {
	res := make(map[string][]string)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPrefix+SlashSeparator) {
		if item.Err != nil {
			return nil, item.Err
		}

		lastIndex := strings.HasPrefix(item.Item, policyDBPrefix)
		listKey, trimmedItem := splitPath(item.Item, lastIndex)
		if listKey == iamFormatFile {
			continue
		}

		res[listKey] = append(res[listKey], trimmedItem)
	}
	// Store the listing for later re-use.
	iamOS.cachedIAMListing.Store(res)
	return res, nil
}

// PurgeExpiredSTS - purge expired STS credentials from object store.
func (iamOS *IAMObjectStore) PurgeExpiredSTS(ctx context.Context) error {
	if iamOS.objAPI == nil {
		return errServerNotInitialized
	}

	bootstrapTraceMsg("purging expired STS credentials")

	iamListing, ok := iamOS.cachedIAMListing.Load().(map[string][]string)
	if !ok {
		// There has been no store yet. This should never happen!
		iamLogIf(GlobalContext, errors.New("WARNING: no cached IAM listing found"))
		return nil
	}

	// Scan STS users on disk and purge expired ones. We do not need to hold a
	// lock with store.lock() here.
	stsAccountsFromStore := map[string]UserIdentity{}
	stsAccPoliciesFromStore := xsync.NewMapOf[string, MappedPolicy]()
	for _, item := range iamListing[stsListKey] {
		userName := path.Dir(item)
		// loadUser() will delete expired user during the load.
		err := iamOS.loadUser(ctx, userName, stsUser, stsAccountsFromStore)
		if err != nil && !errors.Is(err, errNoSuchUser) {
			iamLogIf(GlobalContext,
				fmt.Errorf("unable to load user during STS purge: %w (%s)", err, item))
		}

	}
	// Loading the STS policy mappings from disk ensures that stale entries
	// (removed during loadUser() in the loop above) are removed from memory.
	for _, item := range iamListing[policyDBSTSUsersListKey] {
		stsName := strings.TrimSuffix(item, ".json")
		err := iamOS.loadMappedPolicy(ctx, stsName, stsUser, false, stsAccPoliciesFromStore)
		if err != nil && !errors.Is(err, errNoSuchPolicy) {
			iamLogIf(GlobalContext,
				fmt.Errorf("unable to load policies during STS purge: %w (%s)", err, item))
		}

	}

	// Store the newly populated map in the iam cache. This takes care of
	// removing stale entries from the existing map.
	iamOS.Lock()
	defer iamOS.Unlock()
	iamOS.iamCache.iamSTSAccountsMap = stsAccountsFromStore
	iamOS.iamCache.iamSTSPolicyMap = stsAccPoliciesFromStore

	return nil
}

// Assumes cache is locked by caller.
func (iamOS *IAMObjectStore) loadAllFromObjStore(ctx context.Context, cache *iamCache) error {
	if iamOS.objAPI == nil {
		return errServerNotInitialized
	}

	bootstrapTraceMsg("loading all IAM items")

	listedConfigItems, err := iamOS.listAllIAMConfigItems(ctx)
	if err != nil {
		return fmt.Errorf("unable to list IAM data: %w", err)
	}

	// Loads things in the same order as `LoadIAMCache()`

	bootstrapTraceMsg("loading policy documents")

	policiesList := listedConfigItems[policiesListKey]
	for _, item := range policiesList {
		policyName := path.Dir(item)
		if err := iamOS.loadPolicyDoc(ctx, policyName, cache.iamPolicyDocsMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return fmt.Errorf("unable to load the policy doc `%s`: %w", policyName, err)
		}
	}
	setDefaultCannedPolicies(cache.iamPolicyDocsMap)

	if iamOS.usersSysType == MinIOUsersSysType {
		bootstrapTraceMsg("loading regular IAM users")
		regUsersList := listedConfigItems[usersListKey]
		for _, item := range regUsersList {
			userName := path.Dir(item)
			if err := iamOS.loadUser(ctx, userName, regUser, cache.iamUsersMap); err != nil && err != errNoSuchUser {
				return fmt.Errorf("unable to load the user `%s`: %w", userName, err)
			}
		}

		bootstrapTraceMsg("loading regular IAM groups")
		groupsList := listedConfigItems[groupsListKey]
		for _, item := range groupsList {
			group := path.Dir(item)
			if err := iamOS.loadGroup(ctx, group, cache.iamGroupsMap); err != nil && err != errNoSuchGroup {
				return fmt.Errorf("unable to load the group `%s`: %w", group, err)
			}
		}
	}

	bootstrapTraceMsg("loading user policy mapping")
	userPolicyMappingsList := listedConfigItems[policyDBUsersListKey]
	for _, item := range userPolicyMappingsList {
		userName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, userName, regUser, false, cache.iamUserPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return fmt.Errorf("unable to load the policy mapping for the user `%s`: %w", userName, err)
		}
	}

	bootstrapTraceMsg("loading group policy mapping")
	groupPolicyMappingsList := listedConfigItems[policyDBGroupsListKey]
	for _, item := range groupPolicyMappingsList {
		groupName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, groupName, regUser, true, cache.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return fmt.Errorf("unable to load the policy mapping for the group `%s`: %w", groupName, err)
		}
	}

	bootstrapTraceMsg("loading service accounts")
	svcAccList := listedConfigItems[svcAccListKey]
	svcUsersMap := make(map[string]UserIdentity, len(svcAccList))
	for _, item := range svcAccList {
		userName := path.Dir(item)
		if err := iamOS.loadUser(ctx, userName, svcUser, svcUsersMap); err != nil && err != errNoSuchUser {
			return fmt.Errorf("unable to load the service account `%s`: %w", userName, err)
		}
	}
	for _, svcAcc := range svcUsersMap {
		svcParent := svcAcc.Credentials.ParentUser
		if _, ok := cache.iamUsersMap[svcParent]; !ok {
			// If a service account's parent user is not in iamUsersMap, the
			// parent is an STS account. Such accounts may have a policy mapped
			// on the parent user, so we load them. This is not needed for the
			// initial server startup, however, it is needed for the case where
			// the STS account's policy mapping (for example in LDAP mode) may
			// be changed and the user's policy mapping in memory is stale
			// (because the policy change notification was missed by the current
			// server).
			//
			// The "policy not found" error is ignored because the STS account may
			// not have a policy mapped via its parent (for e.g. in
			// OIDC/AssumeRoleWithCustomToken/AssumeRoleWithCertificate).
			err := iamOS.loadMappedPolicy(ctx, svcParent, stsUser, false, cache.iamSTSPolicyMap)
			if err != nil && !errors.Is(err, errNoSuchPolicy) {
				return fmt.Errorf("unable to load the policy mapping for the STS user `%s`: %w", svcParent, err)
			}
		}
	}
	// Copy svcUsersMap to cache.iamUsersMap
	for k, v := range svcUsersMap {
		cache.iamUsersMap[k] = v
	}

	cache.buildUserGroupMemberships()
	return nil
}

func (iamOS *IAMObjectStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return iamOS.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (iamOS *IAMObjectStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return iamOS.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (iamOS *IAMObjectStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return iamOS.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (iamOS *IAMObjectStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return iamOS.saveIAMConfig(ctx, gi, getGroupInfoPath(name))
}

func (iamOS *IAMObjectStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := iamOS.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := iamOS.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (iamOS *IAMObjectStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := iamOS.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

// Lists objects in the minioMetaBucket at the given path prefix. All returned
// items have the pathPrefix removed from their names.
func listIAMConfigItems(ctx context.Context, objAPI ObjectLayer, pathPrefix string) <-chan itemOrErr[string] {
	ch := make(chan itemOrErr[string])

	go func() {
		defer xioutil.SafeClose(ch)

		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan itemOrErr[ObjectInfo])

		if err := objAPI.Walk(ctx, minioMetaBucket, pathPrefix, objInfoCh, WalkOptions{}); err != nil {
			select {
			case ch <- itemOrErr[string]{Err: err}:
			case <-ctx.Done():
			}
			return
		}

		for obj := range objInfoCh {
			if obj.Err != nil {
				select {
				case ch <- itemOrErr[string]{Err: obj.Err}:
				case <-ctx.Done():
					return
				}
			}
			item := strings.TrimPrefix(obj.Item.Name, pathPrefix)
			item = strings.TrimSuffix(item, SlashSeparator)
			select {
			case ch <- itemOrErr[string]{Item: item}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
