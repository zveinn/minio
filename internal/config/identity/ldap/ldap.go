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

package ldap

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	xldap "github.com/minio/pkg/v2/ldap"
)

// LookupUserDN searches for the full DN and groups of a given short/login
// username.
func (l *Config) LookupUserDN(username string) (string, []string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err := l.LDAP.LookupUserDN(conn, username)
	if err != nil {
		errRet := fmt.Errorf("Unable to find user DN: %w", err)
		return "", nil, errRet
	}

	groups, err := l.LDAP.SearchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
}

// GetValidatedDNForUsername checks if the given username exists in the LDAP directory.
// The given username could be just the short "login" username or the full DN.
//
// When the username/DN is found, the full DN returned by the **server** is
// returned, otherwise the returned string is empty. The value returned here is
// the value sent by the LDAP server and is used in minio as the server performs
// LDAP specific normalization (including Unicode normalization).
//
// If the user is not found, err = nil, otherwise, err != nil.
func (l *Config) GetValidatedDNForUsername(username string) (string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", err
	}

	// Check if the passed in username is a valid DN.
	if !l.ParsesAsDN(username) {
		// We consider it as a login username and attempt to check it exists in
		// the directory.
		bindDN, err := l.LDAP.LookupUserDN(conn, username)
		if err != nil {
			if strings.Contains(err.Error(), "User DN not found for") {
				return "", nil
			}
			return "", fmt.Errorf("Unable to find user DN: %w", err)
		}
		return bindDN, nil
	}

	// Since the username parses as a valid DN, check that it exists and is
	// under a configured base DN in the LDAP directory.
	validDN, isUnderBaseDN, err := l.GetValidatedUserDN(conn, username)
	if err == nil && !isUnderBaseDN {
		return "", fmt.Errorf("Unable to find user DN: %w", err)
	}
	return validDN, err
}

// GetValidatedUserDN validates the given user DN. Will error out if conn is nil. The returned
// boolean is true iff the user DN is found under one of the LDAP user base DNs.
func (l *Config) GetValidatedUserDN(conn *ldap.Conn, userDN string) (string, bool, error) {
	return l.GetValidatedDNUnderBaseDN(conn, userDN, l.LDAP.UserDNSearchBaseDistNames)
}

// GetValidatedGroupDN validates the given group DN. If conn is nil, creates a
// connection. The returned boolean is true iff the group DN is found under one
// of the configured LDAP base DNs.
func (l *Config) GetValidatedGroupDN(conn *ldap.Conn, groupDN string) (string, bool, error) {
	if conn == nil {
		var err error
		conn, err = l.LDAP.Connect()
		if err != nil {
			return "", false, err
		}
		defer conn.Close()

		// Bind to the lookup user account
		if err = l.LDAP.LookupBind(conn); err != nil {
			return "", false, err
		}
	}

	return l.GetValidatedDNUnderBaseDN(conn, groupDN, l.LDAP.GroupSearchBaseDistNames)
}

// GetValidatedDNUnderBaseDN checks if the given DN exists in the LDAP directory
// and returns the DN value sent by the LDAP server. The value returned by the
// server may not be equal to the input DN, as LDAP equality is not a simple
// Golang string equality. However, we assume the value returned by the LDAP
// server is canonical. Additionally, the attribute type names in the DN are
// lower-cased.
//
// Return values:
//
// If the DN is found, the normalized (string) value is returned and error is
// nil.
//
// If the DN is not found, the string returned is empty and the error is nil.
//
// The returned boolean is true iff the DN is found under one of the LDAP
// subtrees listed in `baseDNList`.
func (l *Config) GetValidatedDNUnderBaseDN(conn *ldap.Conn, dn string, baseDNList []xldap.BaseDNInfo) (string, bool, error) {
	if len(baseDNList) == 0 {
		return "", false, errors.New("no Base DNs given")
	}

	// Check that DN exists in the LDAP directory.
	validatedDN, err := xldap.LookupDN(conn, dn)
	if err != nil {
		return "", false, fmt.Errorf("Error looking up DN %s: %w", dn, err)
	}
	if validatedDN == "" {
		return "", false, nil
	}

	// This will not return an error as the argument is validated to be a DN.
	pdn, _ := ldap.ParseDN(validatedDN)

	// Check that the DN is under a configured base DN in the LDAP
	// directory.
	for _, baseDN := range baseDNList {
		if baseDN.Parsed.AncestorOf(pdn) {
			return validatedDN, true, nil
		}
	}

	// Not under any configured base DN so return false.
	return validatedDN, false, nil
}

// Bind - binds to ldap, searches LDAP and returns the distinguished name of the
// user and the list of groups.
func (l *Config) Bind(username, password string) (string, []string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	var bindDN string
	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err = l.LDAP.LookupUserDN(conn, username)
	if err != nil {
		errRet := fmt.Errorf("Unable to find user DN: %w", err)
		return "", nil, errRet
	}

	// Authenticate the user credentials.
	err = conn.Bind(bindDN, password)
	if err != nil {
		errRet := fmt.Errorf("LDAP auth failed for DN %s: %w", bindDN, err)
		return "", nil, errRet
	}

	// Bind to the lookup user account again to perform group search.
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// User groups lookup.
	groups, err := l.LDAP.SearchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
}

// GetExpiryDuration - return parsed expiry duration.
func (l Config) GetExpiryDuration(dsecs string) (time.Duration, error) {
	if dsecs == "" {
		return l.stsExpiryDuration, nil
	}

	d, err := strconv.Atoi(dsecs)
	if err != nil {
		return 0, auth.ErrInvalidDuration
	}

	dur := time.Duration(d) * time.Second

	if dur < minLDAPExpiry || dur > maxLDAPExpiry {
		return 0, auth.ErrInvalidDuration
	}
	return dur, nil
}

// ParsesAsDN determines if the given string could be a valid DN based on
// parsing alone.
func (l Config) ParsesAsDN(dn string) bool {
	_, err := ldap.ParseDN(dn)
	return err == nil
}

// IsLDAPUserDN determines if the given string could be a user DN from LDAP.
func (l Config) IsLDAPUserDN(user string) bool {
	udn, err := ldap.ParseDN(user)
	if err != nil {
		return false
	}
	for _, baseDN := range l.LDAP.UserDNSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(udn) {
			return true
		}
	}
	return false
}

// IsLDAPGroupDN determines if the given string could be a group DN from LDAP.
func (l Config) IsLDAPGroupDN(group string) bool {
	gdn, err := ldap.ParseDN(group)
	if err != nil {
		return false
	}
	for _, baseDN := range l.LDAP.GroupSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(gdn) {
			return true
		}
	}
	return false
}

// GetNonEligibleUserDistNames - find user accounts (DNs) that are no longer
// present in the LDAP server or do not meet filter criteria anymore
func (l *Config) GetNonEligibleUserDistNames(userDistNames []string) ([]string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return nil, err
	}

	// Evaluate the filter again with generic wildcard instead of  specific values
	filter := strings.ReplaceAll(l.LDAP.UserDNSearchFilter, "%s", "*")

	nonExistentUsers := []string{}
	for _, dn := range userDistNames {
		searchRequest := ldap.NewSearchRequest(
			dn,
			ldap.ScopeBaseObject, ldap.NeverDerefAliases, 0, 0, false,
			filter,
			[]string{}, // only need DN, so pass no attributes here
			nil,
		)

		searchResult, err := conn.Search(searchRequest)
		if err != nil {
			// Object does not exist error?
			if ldap.IsErrorWithCode(err, 32) {
				nonExistentUsers = append(nonExistentUsers, dn)
				continue
			}
			return nil, err
		}
		if len(searchResult.Entries) == 0 {
			// DN was not found - this means this user account is
			// expired.
			nonExistentUsers = append(nonExistentUsers, dn)
		}
	}
	return nonExistentUsers, nil
}

// LookupGroupMemberships - for each DN finds the set of LDAP groups they are a
// member of.
func (l *Config) LookupGroupMemberships(userDistNames []string, userDNToUsernameMap map[string]string) (map[string]set.StringSet, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return nil, err
	}

	res := make(map[string]set.StringSet, len(userDistNames))
	for _, userDistName := range userDistNames {
		username := userDNToUsernameMap[userDistName]
		groups, err := l.LDAP.SearchForUserGroups(conn, username, userDistName)
		if err != nil {
			return nil, err
		}
		res[userDistName] = set.CreateStringSet(groups...)
	}

	return res, nil
}
