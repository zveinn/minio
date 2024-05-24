// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/mux"
	"github.com/minio/pkg/v2/policy"
)

// ListLDAPPolicyMappingEntities lists users/groups mapped to given/all policies.
//
// GET <admin-prefix>/idp/ldap/policy-entities?[query-params]
//
// Query params:
//
//	user=... -> repeatable query parameter, specifying users to query for
//	policy mapping
//
//	group=... -> repeatable query parameter, specifying groups to query for
//	policy mapping
//
//	policy=... -> repeatable query parameter, specifying policy to query for
//	user/group mapping
//
// When all query parameters are omitted, returns mappings for all policies.
func (a adminAPIHandlers) ListLDAPPolicyMappingEntities(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check authorization.

	objectAPI, cred := validateAdminReq(ctx, w, r,
		policy.ListGroupsAdminAction, policy.ListUsersAdminAction, policy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	// Validate API arguments.

	q := madmin.PolicyEntitiesQuery{
		Users:  r.Form["user"],
		Groups: r.Form["group"],
		Policy: r.Form["policy"],
	}

	// Query IAM

	res, err := globalIAMSys.QueryLDAPPolicyEntities(r.Context(), q)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Encode result and send response.

	data, err := json.Marshal(res)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, econfigData)
}

// AttachDetachPolicyLDAP attaches or detaches policies from an LDAP entity
// (user or group).
//
// POST <admin-prefix>/idp/ldap/policy/{operation}
func (a adminAPIHandlers) AttachDetachPolicyLDAP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check authorization.

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.UpdatePolicyAssociationAction)
	if objectAPI == nil {
		return
	}

	// fail if ldap is not enabled
	if !globalIAMSys.LDAPConfig.Enabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminLDAPNotEnabled), r.URL)
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	// Ensure body content type is opaque to ensure that request body has not
	// been interpreted as form data.
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/octet-stream" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	// Validate operation
	operation := mux.Vars(r)["operation"]
	if operation != "attach" && operation != "detach" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}

	isAttach := operation == "attach"

	// Validate API arguments in body.
	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var par madmin.PolicyAssociationReq
	err = json.Unmarshal(reqBytes, &par)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := par.IsValid(); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	// Call IAM subsystem
	updatedAt, addedOrRemoved, _, err := globalIAMSys.PolicyDBUpdateLDAP(ctx, isAttach, par)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	respBody := madmin.PolicyAssociationResp{
		UpdatedAt: updatedAt,
	}
	if isAttach {
		respBody.PoliciesAttached = addedOrRemoved
	} else {
		respBody.PoliciesDetached = addedOrRemoved
	}

	data, err := json.Marshal(respBody)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)
}

// AddServiceAccountLDAP adds a new service account for provided LDAP username or DN
//
// PUT /minio/admin/v3/idp/ldap/add-service-account
func (a adminAPIHandlers) AddServiceAccountLDAP(w http.ResponseWriter, r *http.Request) {
	ctx, cred, opts, createReq, targetUser, APIError := commonAddServiceAccount(r)
	if APIError.Code != "" {
		writeErrorResponseJSON(ctx, w, APIError, r.URL)
		return
	}

	// fail if ldap is not enabled
	if !globalIAMSys.LDAPConfig.Enabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminLDAPNotEnabled), r.URL)
		return
	}

	// Find the user for the request sender (as it may be sent via a service
	// account or STS account):
	requestorUser := cred.AccessKey
	requestorParentUser := cred.AccessKey
	requestorGroups := cred.Groups
	requestorIsDerivedCredential := false
	if cred.IsServiceAccount() || cred.IsTemp() {
		requestorParentUser = cred.ParentUser
		requestorIsDerivedCredential = true
	}

	// Check if we are creating svc account for request sender.
	isSvcAccForRequestor := false
	if targetUser == requestorUser || targetUser == requestorParentUser {
		isSvcAccForRequestor = true
	}

	var (
		targetGroups []string
		err          error
	)

	// If we are creating svc account for request sender, ensure that targetUser
	// is a real user (i.e. not derived credentials).
	if isSvcAccForRequestor {
		if requestorIsDerivedCredential {
			if requestorParentUser == "" {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx,
					errors.New("service accounts cannot be generated for temporary credentials without parent")), r.URL)
				return
			}
			targetUser = requestorParentUser
		}
		targetGroups = requestorGroups

		// Deny if the target user is not LDAP
		foundLDAPDN, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(targetUser)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if foundLDAPDN == "" {
			err := errors.New("Specified user does not exist on LDAP server")
			APIErr := errorCodes.ToAPIErrWithErr(ErrAdminNoSuchUser, err)
			writeErrorResponseJSON(ctx, w, APIErr, r.URL)
			return
		}

		// In case of LDAP/OIDC we need to set `opts.claims` to ensure
		// it is associated with the LDAP/OIDC user properly.
		for k, v := range cred.Claims {
			if k == expClaim {
				continue
			}
			opts.claims[k] = v
		}
	} else {
		// We still need to ensure that the target user is a valid LDAP user.
		//
		// The target user may be supplied as a (short) username or a DN.
		// However, for now, we only support using the short username.

		isDN := globalIAMSys.LDAPConfig.ParsesAsDN(targetUser)
		opts.claims[ldapUserN] = targetUser // simple username
		targetUser, targetGroups, err = globalIAMSys.LDAPConfig.LookupUserDN(targetUser)
		if err != nil {
			// if not found, check if DN
			if strings.Contains(err.Error(), "User DN not found for:") {
				if isDN {
					// warn user that DNs are not allowed
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminLDAPExpectedLoginName, err), r.URL)
				} else {
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminNoSuchUser, err), r.URL)
				}
			}
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		opts.claims[ldapUser] = targetUser // DN
	}

	newCred, updatedAt, err := globalIAMSys.NewServiceAccount(ctx, targetUser, targetGroups, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	createResp := madmin.AddServiceAccountResp{
		Credentials: madmin.Credentials{
			AccessKey:  newCred.AccessKey,
			SecretKey:  newCred.SecretKey,
			Expiration: newCred.Expiration,
		},
	}

	data, err := json.Marshal(createResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(cred.SecretKey, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)

	// Call hook for cluster-replication if the service account is not for a
	// root user.
	if newCred.ParentUser != globalActiveCred.AccessKey {
		replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Create: &madmin.SRSvcAccCreate{
					Parent:        newCred.ParentUser,
					AccessKey:     newCred.AccessKey,
					SecretKey:     newCred.SecretKey,
					Groups:        newCred.Groups,
					Name:          newCred.Name,
					Description:   newCred.Description,
					Claims:        opts.claims,
					SessionPolicy: createReq.Policy,
					Status:        auth.AccountOn,
					Expiration:    createReq.Expiration,
				},
			},
			UpdatedAt: updatedAt,
		}))
	}
}

// ListAccessKeysLDAP - GET /minio/admin/v3/idp/ldap/list-access-keys
func (a adminAPIHandlers) ListAccessKeysLDAP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	userDN := r.Form.Get("userDN")

	// If listing is requested for a specific user (who is not the request
	// sender), check that the user has permissions.
	if userDN != "" && userDN != cred.ParentUser {
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListServiceAccountsAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	} else {
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListServiceAccountsAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
			DenyOnly:        true,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
		userDN = cred.AccessKey
		if cred.ParentUser != "" {
			userDN = cred.ParentUser
		}
	}

	targetAccount, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(userDN)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if targetAccount == "" {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errNoSuchUser), r.URL)
		return
	}

	listType := r.Form.Get("listType")
	if listType != "sts-only" && listType != "svcacc-only" && listType != "" {
		// default to both
		listType = ""
	}

	var serviceAccounts []auth.Credentials
	var stsKeys []auth.Credentials

	if listType == "" || listType == "sts-only" {
		stsKeys, err = globalIAMSys.ListSTSAccounts(ctx, targetAccount)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	if listType == "" || listType == "svcacc-only" {
		serviceAccounts, err = globalIAMSys.ListServiceAccounts(ctx, targetAccount)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	var serviceAccountList []madmin.ServiceAccountInfo
	var stsKeyList []madmin.ServiceAccountInfo

	for _, svc := range serviceAccounts {
		expiryTime := svc.Expiration
		serviceAccountList = append(serviceAccountList, madmin.ServiceAccountInfo{
			AccessKey:  svc.AccessKey,
			Expiration: &expiryTime,
		})
	}
	for _, sts := range stsKeys {
		expiryTime := sts.Expiration
		stsKeyList = append(stsKeyList, madmin.ServiceAccountInfo{
			AccessKey:  sts.AccessKey,
			Expiration: &expiryTime,
		})
	}

	listResp := madmin.ListAccessKeysLDAPResp{
		ServiceAccounts: serviceAccountList,
		STSKeys:         stsKeyList,
	}

	data, err := json.Marshal(listResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(cred.SecretKey, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)
}
