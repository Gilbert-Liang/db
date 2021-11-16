package meta

import (
	"db"
	internal "db/meta/internal"
	"db/model"
	"db/parser/cnosql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	// DefaultTimeToLiveReplicaN is the default value of TimeToLiveInfo.ReplicaN.
	DefaultTimeToLiveReplicaN = 1

	// DefaultTimeToLiveDuration is the default value of TimeToLiveInfo.Duration.
	DefaultTimeToLiveDuration = time.Duration(0)

	// DefaultTimeToLiveName is the default name for auto generated time-to-lives.
	DefaultTimeToLiveName = "autogen"

	// MinTimeToLiveDuration represents the minimum duration for a time-to-live.
	MinTimeToLiveDuration = time.Hour

	// MaxNameLen is the maximum length of a database or time-to-live name.
	// CnosDB uses the name for the directory name on disk.
	MaxNameLen = 255
)

// Data represents the top level collection of all metadata.
type Data struct {
	Term      uint64 // associated raft term
	Index     uint64 // associated raft index
	ClusterID uint64
	Databases []DatabaseInfo
	Users     []UserInfo

	// adminUserExists provides a constant time mechanism for determining
	// if there is at least one admin user.
	adminUserExists bool

	MaxNodeID   uint64
	MaxRegionID uint64
	MaxShardID  uint64
}

// Database returns a DatabaseInfo by the database name.
func (data *Data) Database(name string) *DatabaseInfo {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			return &data.Databases[i]
		}
	}
	return nil
}

// CreateDatabase creates a new database.
// It returns an error if name is blank or if a database with the same name already exists.
func (data *Data) CreateDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	} else if len(name) > MaxNameLen {
		return ErrNameTooLong
	} else if data.Database(name) != nil {
		return nil
	}

	// Append new node.
	data.Databases = append(data.Databases, DatabaseInfo{Name: name})

	return nil
}

// DropDatabase removes a database by name. It does not return an error
// if the database cannot be found.
func (data *Data) DropDatabase(name string) error {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			data.Databases = append(data.Databases[:i], data.Databases[i+1:]...)

			// Remove all user privileges associated with this database.
			for i := range data.Users {
				delete(data.Users[i].Privileges, name)
			}
			break
		}
	}
	return nil
}

// TimeToLive returns a time-to-live for a database by name.
func (data *Data) TimeToLive(database, name string) (*TimeToLiveInfo, error) {
	di := data.Database(database)
	if di == nil {
		return nil, db.ErrDatabaseNotFound(database)
	}

	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			return &di.TimeToLives[i], nil
		}
	}
	return nil, nil
}

// CreateTimeToLive creates a new time-to-live on a database.
// It returns an error if name is blank or if the database does not exist.
func (data *Data) CreateTimeToLive(database string, ttlInfo *TimeToLiveInfo, makeDefault bool) error {
	// Validate time-to-live.
	if ttlInfo == nil {
		return ErrTimeToLiveRequired
	} else if ttlInfo.Name == "" {
		return ErrTimeToLiveNameRequired
	} else if len(ttlInfo.Name) > MaxNameLen {
		return ErrNameTooLong
	} else if ttlInfo.ReplicaN < 1 {
		ttlInfo.ReplicaN = 1
	}

	// Normalise ShardDuration before comparing to any existing
	// time-to-lives. The client is supposed to do this, but
	// do it again to verify input.
	ttlInfo.RegionDuration = normalisedShardDuration(ttlInfo.RegionDuration, ttlInfo.Duration)

	if ttlInfo.Duration > 0 && ttlInfo.Duration < ttlInfo.RegionDuration {
		return ErrIncompatibleDurations
	}

	// Find database.
	di := data.Database(database)
	if di == nil {
		return db.ErrDatabaseNotFound(database)
	} else if ttl := di.TimeToLive(ttlInfo.Name); ttl != nil {
		// Time-to-live with that name already exists. Make sure they're the same.
		if ttl.ReplicaN != ttlInfo.ReplicaN || ttl.Duration != ttlInfo.Duration || ttl.RegionDuration != ttlInfo.RegionDuration {
			return ErrTimeToLiveExists
		}
		// if they want to make it default, and it's not the default, it's not an identical command so it's an error
		if makeDefault && di.DefaultTimeToLive != ttlInfo.Name {
			return ErrTimeToLiveConflict
		}
		return nil
	}

	// Append copy of new time-to-live.
	di.TimeToLives = append(di.TimeToLives, *ttlInfo)

	// Set the default if needed
	if makeDefault {
		di.DefaultTimeToLive = ttlInfo.Name
	}

	return nil
}

// DropTimeToLive removes a time-to-live from a database by name.
func (data *Data) DropTimeToLive(database, name string) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		// no database? no problem
		return nil
	}

	// Remove from list.
	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			di.TimeToLives = append(di.TimeToLives[:i], di.TimeToLives[i+1:]...)
			break
		}
	}

	return nil
}

// TimeToLiveUpdate represents time-to-live fields to be updated.
type TimeToLiveUpdate struct {
	Name           *string
	Duration       *time.Duration
	ReplicaN       *int
	RegionDuration *time.Duration
}

// SetName sets the TimeToLiveUpdate.Name.
func (t *TimeToLiveUpdate) SetName(v string) { t.Name = &v }

// SetDuration sets the TimeToLiveUpdate.Duration.
func (t *TimeToLiveUpdate) SetDuration(v time.Duration) { t.Duration = &v }

// SetReplicaN sets the TimeToLiveUpdate.ReplicaN.
func (t *TimeToLiveUpdate) SetReplicaN(v int) { t.ReplicaN = &v }

// SetRegionDuration sets the TimeToLiveUpdate.RegionDuration.
func (t *TimeToLiveUpdate) SetRegionDuration(v time.Duration) { t.RegionDuration = &v }

// UpdateTimeToLive updates an existing time-to-live.
func (data *Data) UpdateTimeToLive(database, name string, ttl *TimeToLiveUpdate, makeDefault bool) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		return db.ErrDatabaseNotFound(database)
	}

	// Find time-to-live.
	ttlInfo := di.TimeToLive(name)
	if ttlInfo == nil {
		return db.ErrTimeToLiveNotFound(name)
	}

	// Ensure new time-to-live doesn't match an existing time-to-live.
	if ttl.Name != nil && *ttl.Name != name && di.TimeToLive(*ttl.Name) != nil {
		return ErrTimeToLiveNameExists
	}

	// Enforce duration of at least MinTimeToLiveDuration
	if ttl.Duration != nil && *ttl.Duration < MinTimeToLiveDuration && *ttl.Duration != 0 {
		return ErrTimeToLiveDurationTooLow
	}

	// Enforce duration is at least the shard duration
	if (ttl.Duration != nil && *ttl.Duration > 0 &&
		((ttl.RegionDuration != nil && *ttl.Duration < *ttl.RegionDuration) ||
			(ttl.RegionDuration == nil && *ttl.Duration < ttlInfo.RegionDuration))) ||
		(ttl.Duration == nil && ttlInfo.Duration > 0 &&
			ttl.RegionDuration != nil && ttlInfo.Duration < *ttl.RegionDuration) {
		return ErrIncompatibleDurations
	}

	// Update fields.
	if ttl.Name != nil {
		ttlInfo.Name = *ttl.Name
	}
	if ttl.Duration != nil {
		ttlInfo.Duration = *ttl.Duration
	}
	if ttl.ReplicaN != nil {
		ttlInfo.ReplicaN = *ttl.ReplicaN
	}
	if ttl.RegionDuration != nil {
		ttlInfo.RegionDuration = normalisedShardDuration(*ttl.RegionDuration, ttlInfo.Duration)
	}

	if di.DefaultTimeToLive != ttlInfo.Name && makeDefault {
		di.DefaultTimeToLive = ttlInfo.Name
	}

	return nil
}

// SetDefaultTimeToLive sets the default time-to-live for a database.
func (data *Data) SetDefaultTimeToLive(database, name string) error {
	// Find database and verify time-to-live exists.
	di := data.Database(database)
	if di == nil {
		return db.ErrDatabaseNotFound(database)
	} else if di.TimeToLive(name) == nil {
		return db.ErrTimeToLiveNotFound(name)
	}

	// Set default time-to-live.
	di.DefaultTimeToLive = name

	return nil
}

// DropShard removes a shard by ID.
//
// DropShard won't return an error if the shard can't be found, which
// allows the command to be re-run in the case that the meta store
// succeeds but a data node fails.
func (data *Data) DropShard(id uint64) {
	found := -1
	for di, dbInfo := range data.Databases {
		for ti, ttlInfo := range dbInfo.TimeToLives {
			for ri, rgInfo := range ttlInfo.Regions {
				for si, sh := range rgInfo.Shards {
					if sh.ID == id {
						found = si
						break
					}
				}

				if found > -1 {
					shards := rgInfo.Shards
					data.Databases[di].TimeToLives[ti].Regions[ri].Shards = append(shards[:found], shards[found+1:]...)

					if len(shards) == 1 {
						// We just deleted the last shard in the region.
						data.Databases[di].TimeToLives[ti].Regions[ri].DeletedAt = time.Now()
					}
					return
				}
			}
		}
	}
}

// Regions returns a list of all regions on a database and time-to-live.
func (data *Data) Regions(database, ttl string) ([]RegionInfo, error) {
	// Find time-to-live.
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttlInfo == nil {
		return nil, db.ErrTimeToLiveNotFound(ttl)
	}
	regions := make([]RegionInfo, 0, len(ttlInfo.Regions))
	for _, g := range ttlInfo.Regions {
		if g.Deleted() {
			continue
		}
		regions = append(regions, g)
	}
	return regions, nil
}

// RegionsByTimeRange returns a list of all regions on a database and time-to-live that may contain data
// for the specified time range. Regions are sorted by start time.
func (data *Data) RegionsByTimeRange(database, ttl string, tmin, tmax time.Time) ([]RegionInfo, error) {
	// Find time-to-live.
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttlInfo == nil {
		return nil, db.ErrTimeToLiveNotFound(ttl)
	}
	regions := make([]RegionInfo, 0, len(ttlInfo.Regions))
	for _, g := range ttlInfo.Regions {
		if g.Deleted() || !g.Overlaps(tmin, tmax) {
			continue
		}
		regions = append(regions, g)
	}
	return regions, nil
}

// RegionByTimestamp returns the region on a database and time-to-live for a given timestamp.
func (data *Data) RegionByTimestamp(database, ttl string, timestamp time.Time) (*RegionInfo, error) {
	// Find time-to-live.
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttlInfo == nil {
		return nil, db.ErrTimeToLiveNotFound(ttl)
	}

	return ttlInfo.RegionByTimestamp(timestamp), nil
}

// CreateRegion creates a region on a database and time-to-live for a given timestamp.
func (data *Data) CreateRegion(database, ttl string, timestamp time.Time, shards ...ShardInfo) error {
	// Find time-to-live.
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttlInfo == nil {
		return db.ErrTimeToLiveNotFound(ttl)
	}

	// Verify that region doesn't already exist for this timestamp.
	if ttlInfo.RegionByTimestamp(timestamp) != nil {
		return nil
	}

	// Create the region.
	data.MaxRegionID++
	sgi := RegionInfo{}
	sgi.ID = data.MaxRegionID
	sgi.StartTime = timestamp.Truncate(ttlInfo.RegionDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(ttlInfo.RegionDuration).UTC()
	if sgi.EndTime.After(time.Unix(0, model.MaxNanoTime)) {
		// Region range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, model.MaxNanoTime+1)
	}

	if len(shards) > 0 {
		sgi.Shards = make([]ShardInfo, len(shards))
		for i, si := range shards {
			sgi.Shards[i] = si
			if si.ID > data.MaxShardID {
				data.MaxShardID = si.ID
			}
		}
	} else {
		data.MaxShardID++
		sgi.Shards = []ShardInfo{
			{ID: data.MaxShardID},
		}
	}

	// Time-to-live has a new region, so update the time-to-live. Regions
	// must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	ttlInfo.Regions = append(ttlInfo.Regions, sgi)
	sort.Sort(RegionInfos(ttlInfo.Regions))

	return nil
}

// DeleteRegion removes a region from a database and time-to-live by id.
func (data *Data) DeleteRegion(database, ttl string, id uint64) error {
	// Find time-to-live.
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttlInfo == nil {
		return db.ErrTimeToLiveNotFound(ttl)
	}

	// Find region by ID and set its deletion timestamp.
	for i := range ttlInfo.Regions {
		if ttlInfo.Regions[i].ID == id {
			ttlInfo.Regions[i].DeletedAt = time.Now().UTC()
			return nil
		}
	}

	return ErrRegionNotFound
}

// CreateContinuousQuery adds a named continuous query to a database.
func (data *Data) CreateContinuousQuery(database, name, query string) error {
	di := data.Database(database)
	if di == nil {
		return db.ErrDatabaseNotFound(database)
	}

	// Ensure the name doesn't already exist.
	for _, cq := range di.ContinuousQueries {
		if cq.Name == name {
			// If the query string is the same, we'll silently return,
			// otherwise we'll assume the user might be trying to
			// overwrite an existing CQ with a different query.
			if strings.ToLower(cq.Query) == strings.ToLower(query) {
				return nil
			}
			return ErrContinuousQueryExists
		}
	}

	// Append new query.
	di.ContinuousQueries = append(di.ContinuousQueries, ContinuousQueryInfo{
		Name:  name,
		Query: query,
	})

	return nil
}

// DropContinuousQuery removes a continuous query.
func (data *Data) DropContinuousQuery(database, name string) error {
	di := data.Database(database)
	if di == nil {
		return nil
	}

	for i := range di.ContinuousQueries {
		if di.ContinuousQueries[i].Name == name {
			di.ContinuousQueries = append(di.ContinuousQueries[:i], di.ContinuousQueries[i+1:]...)
			return nil
		}
	}
	return nil
}

// validateURL returns an error if the URL does not have a port or uses a scheme other than UDP or HTTP.
func validateURL(input string) error {
	u, err := url.Parse(input)
	if err != nil {
		return ErrInvalidSubscriptionURL(input)
	}

	if u.Scheme != "udp" && u.Scheme != "http" && u.Scheme != "https" {
		return ErrInvalidSubscriptionURL(input)
	}

	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return ErrInvalidSubscriptionURL(input)
	}

	return nil
}

// CreateSubscription adds a named subscription to a database and time-to-live.
func (data *Data) CreateSubscription(database, ttl, name, mode string, destinations []string) error {
	for _, d := range destinations {
		if err := validateURL(d); err != nil {
			return err
		}
	}

	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttlInfo == nil {
		return db.ErrTimeToLiveNotFound(ttl)
	}

	// Ensure the name doesn't already exist.
	for i := range ttlInfo.Subscriptions {
		if ttlInfo.Subscriptions[i].Name == name {
			return ErrSubscriptionExists
		}
	}

	// Append new query.
	ttlInfo.Subscriptions = append(ttlInfo.Subscriptions, SubscriptionInfo{
		Name:         name,
		Mode:         mode,
		Destinations: destinations,
	})

	return nil
}

// DropSubscription removes a subscription.
func (data *Data) DropSubscription(database, ttl, name string) error {
	ttlInfo, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttlInfo == nil {
		return db.ErrTimeToLiveNotFound(ttl)
	}

	for i := range ttlInfo.Subscriptions {
		if ttlInfo.Subscriptions[i].Name == name {
			ttlInfo.Subscriptions = append(ttlInfo.Subscriptions[:i], ttlInfo.Subscriptions[i+1:]...)
			return nil
		}
	}
	return ErrSubscriptionNotFound
}

func (data *Data) user(username string) *UserInfo {
	for i := range data.Users {
		if data.Users[i].Name == username {
			return &data.Users[i]
		}
	}
	return nil
}

// User returns a user by username.
func (data *Data) User(username string) User {
	u := data.user(username)
	if u == nil {
		// prevent non-nil interface with nil pointer
		return nil
	}
	return u
}

// CreateUser creates a new user.
func (data *Data) CreateUser(name, hash string, admin bool) error {
	// Ensure the user doesn't already exist.
	if name == "" {
		return ErrUsernameRequired
	} else if data.User(name) != nil {
		return ErrUserExists
	}

	// Append new user.
	data.Users = append(data.Users, UserInfo{
		Name:  name,
		Hash:  hash,
		Admin: admin,
	})

	// We know there is now at least one admin user.
	if admin {
		data.adminUserExists = true
	}

	return nil
}

// DropUser removes an existing user by name.
func (data *Data) DropUser(name string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			wasAdmin := data.Users[i].Admin
			data.Users = append(data.Users[:i], data.Users[i+1:]...)

			// Maybe we dropped the only admin user?
			if wasAdmin {
				data.adminUserExists = data.hasAdminUser()
			}
			return nil
		}
	}

	return ErrUserNotFound
}

// UpdateUser updates the password hash of an existing user.
func (data *Data) UpdateUser(name, hash string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users[i].Hash = hash
			return nil
		}
	}
	return ErrUserNotFound
}

// SetPrivilege sets a privilege for a user on a database.
func (data *Data) SetPrivilege(name, database string, p cnosql.Privilege) error {
	ui := data.user(name)
	if ui == nil {
		return ErrUserNotFound
	}

	if data.Database(database) == nil {
		return db.ErrDatabaseNotFound(database)
	}

	if ui.Privileges == nil {
		ui.Privileges = make(map[string]cnosql.Privilege)
	}
	ui.Privileges[database] = p

	return nil
}

// SetAdminPrivilege sets the admin privilege for a user.
func (data *Data) SetAdminPrivilege(name string, admin bool) error {
	ui := data.user(name)
	if ui == nil {
		return ErrUserNotFound
	}

	ui.Admin = admin

	// We could have promoted or revoked the only admin. Check if an admin
	// user exists.
	data.adminUserExists = data.hasAdminUser()
	return nil
}

// AdminUserExists returns true if an admin user exists.
func (data Data) AdminUserExists() bool {
	return data.adminUserExists
}

// UserPrivileges gets the privileges for a user.
func (data *Data) UserPrivileges(name string) (map[string]cnosql.Privilege, error) {
	ui := data.user(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	return ui.Privileges, nil
}

// UserPrivilege gets the privilege for a user on a database.
func (data *Data) UserPrivilege(name, database string) (*cnosql.Privilege, error) {
	ui := data.user(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	for db, p := range ui.Privileges {
		if db == database {
			return &p, nil
		}
	}

	return cnosql.NewPrivilege(cnosql.NoPrivileges), nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data

	if data.Databases != nil {
		other.Databases = make([]DatabaseInfo, len(data.Databases))
		for i := range data.Databases {
			other.Databases[i] = data.Databases[i].clone()
		}
	}

	if data.Users != nil {
		other.Users = make([]UserInfo, len(data.Users))
		for i := range data.Users {
			other.Users[i] = data.Users[i].clone()
		}
	}

	return &other
}

// marshal serializes data to a protobuf representation.
func (data *Data) marshal() *internal.Data {
	pb := &internal.Data{
		Term:      proto.Uint64(data.Term),
		Index:     proto.Uint64(data.Index),
		ClusterID: proto.Uint64(data.ClusterID),

		MaxNodeID:   proto.Uint64(data.MaxNodeID),
		MaxRegionID: proto.Uint64(data.MaxRegionID),
		MaxShardID:  proto.Uint64(data.MaxShardID),
	}

	pb.Databases = make([]*internal.DatabaseInfo, len(data.Databases))
	for i := range data.Databases {
		pb.Databases[i] = data.Databases[i].marshal()
	}

	pb.Users = make([]*internal.UserInfo, len(data.Users))
	for i := range data.Users {
		pb.Users[i] = data.Users[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) unmarshal(pb *internal.Data) {
	data.Term = pb.GetTerm()
	data.Index = pb.GetIndex()
	data.ClusterID = pb.GetClusterID()

	data.MaxNodeID = pb.GetMaxNodeID()
	data.MaxRegionID = pb.GetMaxRegionID()
	data.MaxShardID = pb.GetMaxShardID()

	data.Databases = make([]DatabaseInfo, len(pb.GetDatabases()))
	for i, x := range pb.GetDatabases() {
		data.Databases[i].unmarshal(x)
	}

	data.Users = make([]UserInfo, len(pb.GetUsers()))
	for i, x := range pb.GetUsers() {
		data.Users[i].unmarshal(x)
	}

	// Exhaustively determine if there is an admin user. The marshalled cache
	// value may not be correct.
	data.adminUserExists = data.hasAdminUser()
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinary() ([]byte, error) {
	return proto.Marshal(data.marshal())
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
	var pb internal.Data
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	data.unmarshal(&pb)
	return nil
}

// TruncateRegions truncates any region that could contain timestamps beyond t.
func (data *Data) TruncateRegions(t time.Time) {
	for i := range data.Databases {
		dbInfo := &data.Databases[i]

		for j := range dbInfo.TimeToLives {
			ttlInfo := &dbInfo.TimeToLives[j]

			for k := range ttlInfo.Regions {
				rgInfo := &ttlInfo.Regions[k]

				if !t.Before(rgInfo.EndTime) || rgInfo.Deleted() || (rgInfo.Truncated() && rgInfo.TruncatedAt.Before(t)) {
					continue
				}

				if !t.After(rgInfo.StartTime) {
					// future region
					rgInfo.TruncatedAt = rgInfo.StartTime
				} else {
					rgInfo.TruncatedAt = t
				}
			}
		}
	}
}

// hasAdminUser exhaustively checks for the presence of at least one admin
// user.
func (data *Data) hasAdminUser() bool {
	for _, u := range data.Users {
		if u.Admin {
			return true
		}
	}
	return false
}

// ImportData imports selected data into the current metadata.
// if non-empty, backupDBName, restoreDBName, backupTTLName, restoreTTLName can be used to select DB metadata from other,
// and to assign a new name to the imported data.  Returns a map of shard ID's in the old metadata to new shard ID's
// in the new metadata, along with a list of new databases created, both of which can assist in the import of existing
// shard data during a database restore.
func (data *Data) ImportData(other Data, backupDBName, restoreDBName, backupTTLName, restoreTTLName string) (map[uint64]uint64, []string, error) {
	shardIDMap := make(map[uint64]uint64)
	if backupDBName != "" {
		dbName, err := data.importOneDB(other, backupDBName, restoreDBName, backupTTLName, restoreTTLName, shardIDMap)
		if err != nil {
			return nil, nil, err
		}

		return shardIDMap, []string{dbName}, nil
	}

	// if no backupDBName then we'll try to import all the DB's.  If one of them fails, we'll mark the whole
	// operation a failure and return an error.
	var newDBs []string
	for _, dbi := range other.Databases {
		if dbi.Name == "_internal" {
			continue
		}
		dbName, err := data.importOneDB(other, dbi.Name, "", "", "", shardIDMap)
		if err != nil {
			return nil, nil, err
		}
		newDBs = append(newDBs, dbName)
	}
	return shardIDMap, newDBs, nil
}

// importOneDB imports a single database/time-to-live from an external metadata object, renaming them if new names are provided.
func (data *Data) importOneDB(other Data, backupDBName, restoreDBName, backupTTLName, restoreTTLName string, shardIDMap map[uint64]uint64) (string, error) {

	dbPtr := other.Database(backupDBName)
	if dbPtr == nil {
		return "", fmt.Errorf("imported metadata does not have datbase named %s", backupDBName)
	}

	if restoreDBName == "" {
		restoreDBName = backupDBName
	}

	if data.Database(restoreDBName) != nil {
		return "", errors.New("database already exists")
	}

	// change the names if we want/need to
	err := data.CreateDatabase(restoreDBName)
	if err != nil {
		return "", err
	}
	dbImport := data.Database(restoreDBName)

	if backupTTLName != "" {
		ttlPtr := dbPtr.TimeToLive(backupTTLName)

		if ttlPtr != nil {
			ttlImport := ttlPtr.clone()
			if restoreTTLName == "" {
				restoreTTLName = backupTTLName
			}
			ttlImport.Name = restoreTTLName
			dbImport.TimeToLives = []TimeToLiveInfo{ttlImport}
			dbImport.DefaultTimeToLive = restoreTTLName
		} else {
			return "", fmt.Errorf("time to live not found in meta backup: %s.%s", backupDBName, backupTTLName)
		}

	} else { // import all time-to-lives without renaming
		dbImport.DefaultTimeToLive = dbPtr.DefaultTimeToLive
		if dbPtr.TimeToLives != nil {
			dbImport.TimeToLives = make([]TimeToLiveInfo, len(dbPtr.TimeToLives))
			for i := range dbPtr.TimeToLives {
				dbImport.TimeToLives[i] = dbPtr.TimeToLives[i].clone()
			}
		}

	}

	// renumber the regions and shards for the new time to live(ies)
	for _, ttlImport := range dbImport.TimeToLives {
		for j, sgImport := range ttlImport.Regions {
			data.MaxRegionID++
			ttlImport.Regions[j].ID = data.MaxRegionID
			for k := range sgImport.Shards {
				data.MaxShardID++
				shardIDMap[sgImport.Shards[k].ID] = data.MaxShardID
				sgImport.Shards[k].ID = data.MaxShardID
				// OSS doesn't use Owners but if we are importing this from Enterprise, we'll want to clear it out
				// to avoid any issues if they ever export this DB again to bring back to Enterprise.
				sgImport.Shards[k].Owners = []ShardOwner{}
			}
		}
	}

	return restoreDBName, nil
}

// DatabaseInfo represents information about a database in the system.
type DatabaseInfo struct {
	Name              string
	DefaultTimeToLive string
	TimeToLives       []TimeToLiveInfo
	ContinuousQueries []ContinuousQueryInfo
}

// TimeToLive returns a time-to-live by name.
func (di DatabaseInfo) TimeToLive(name string) *TimeToLiveInfo {
	if name == "" {
		if di.DefaultTimeToLive == "" {
			return nil
		}
		name = di.DefaultTimeToLive
	}

	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			return &di.TimeToLives[i]
		}
	}
	return nil
}

// ShardInfos returns a list of all shards' info for the database.
func (di DatabaseInfo) ShardInfos() []ShardInfo {
	shards := map[uint64]*ShardInfo{}
	for i := range di.TimeToLives {
		for j := range di.TimeToLives[i].Regions {
			rg := di.TimeToLives[i].Regions[j]
			// Skip deleted regions
			if rg.Deleted() {
				continue
			}
			for k := range rg.Shards {
				si := &di.TimeToLives[i].Regions[j].Shards[k]
				shards[si.ID] = si
			}
		}
	}

	infos := make([]ShardInfo, 0, len(shards))
	for _, info := range shards {
		infos = append(infos, *info)
	}

	return infos
}

// clone returns a deep copy of di.
func (di DatabaseInfo) clone() DatabaseInfo {
	other := di

	if di.TimeToLives != nil {
		other.TimeToLives = make([]TimeToLiveInfo, len(di.TimeToLives))
		for i := range di.TimeToLives {
			other.TimeToLives[i] = di.TimeToLives[i].clone()
		}
	}

	// Copy continuous queries.
	if di.ContinuousQueries != nil {
		other.ContinuousQueries = make([]ContinuousQueryInfo, len(di.ContinuousQueries))
		for i := range di.ContinuousQueries {
			other.ContinuousQueries[i] = di.ContinuousQueries[i].clone()
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (di DatabaseInfo) marshal() *internal.DatabaseInfo {
	pb := &internal.DatabaseInfo{}
	pb.Name = proto.String(di.Name)
	pb.DefaultTimeToLive = proto.String(di.DefaultTimeToLive)

	pb.TimeToLives = make([]*internal.TimeToLiveInfo, len(di.TimeToLives))
	for i := range di.TimeToLives {
		pb.TimeToLives[i] = di.TimeToLives[i].marshal()
	}

	pb.ContinuousQueries = make([]*internal.ContinuousQueryInfo, len(di.ContinuousQueries))
	for i := range di.ContinuousQueries {
		pb.ContinuousQueries[i] = di.ContinuousQueries[i].marshal()
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (di *DatabaseInfo) unmarshal(pb *internal.DatabaseInfo) {
	di.Name = pb.GetName()
	di.DefaultTimeToLive = pb.GetDefaultTimeToLive()

	if len(pb.GetTimeToLives()) > 0 {
		di.TimeToLives = make([]TimeToLiveInfo, len(pb.GetTimeToLives()))
		for i, x := range pb.GetTimeToLives() {
			di.TimeToLives[i].unmarshal(x)
		}
	}

	if len(pb.GetContinuousQueries()) > 0 {
		di.ContinuousQueries = make([]ContinuousQueryInfo, len(pb.GetContinuousQueries()))
		for i, x := range pb.GetContinuousQueries() {
			di.ContinuousQueries[i].unmarshal(x)
		}
	}
}

// TimeToLiveSpec represents the specification for a new time-to-live.
type TimeToLiveSpec struct {
	Name           string
	ReplicaN       *int
	Duration       *time.Duration
	RegionDuration time.Duration
}

// NewTimeToLiveInfo creates a new time-to-live info from the specification.
func (s *TimeToLiveSpec) NewTimeToLiveInfo() *TimeToLiveInfo {
	return DefaultTimeToLiveInfo().Apply(s)
}

// Matches checks if this time-to-live specification matches
// an existing time-to-live.
func (s *TimeToLiveSpec) Matches(ttl *TimeToLiveInfo) bool {
	if ttl == nil {
		return false
	} else if s.Name != "" && s.Name != ttl.Name {
		return false
	} else if s.Duration != nil && *s.Duration != ttl.Duration {
		return false
	} else if s.ReplicaN != nil && *s.ReplicaN != ttl.ReplicaN {
		return false
	}

	// Normalise ShardDuration before comparing to any existing time-to-live.
	// Normalize with the time-to-live info's duration instead of the spec
	// since they should be the same and we're performing a comparison.
	sgDuration := normalisedShardDuration(s.RegionDuration, ttl.Duration)
	return sgDuration == ttl.RegionDuration
}

// marshal serializes to a protobuf representation.
func (s *TimeToLiveSpec) marshal() *internal.TimeToLiveSpec {
	pb := &internal.TimeToLiveSpec{}
	if s.Name != "" {
		pb.Name = proto.String(s.Name)
	}
	if s.Duration != nil {
		pb.Duration = proto.Int64(int64(*s.Duration))
	}
	if s.RegionDuration > 0 {
		pb.RegionDuration = proto.Int64(int64(s.RegionDuration))
	}
	if s.ReplicaN != nil {
		pb.ReplicaN = proto.Uint32(uint32(*s.ReplicaN))
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (s *TimeToLiveSpec) unmarshal(pb *internal.TimeToLiveSpec) {
	if pb.Name != nil {
		s.Name = pb.GetName()
	}
	if pb.Duration != nil {
		duration := time.Duration(pb.GetDuration())
		s.Duration = &duration
	}
	if pb.RegionDuration != nil {
		s.RegionDuration = time.Duration(pb.GetRegionDuration())
	}
	if pb.ReplicaN != nil {
		replicaN := int(pb.GetReplicaN())
		s.ReplicaN = &replicaN
	}
}

// MarshalBinary encodes TimeToLiveSpec to a binary format.
func (s *TimeToLiveSpec) MarshalBinary() ([]byte, error) {
	return proto.Marshal(s.marshal())
}

// UnmarshalBinary decodes TimeToLiveSpec from a binary format.
func (s *TimeToLiveSpec) UnmarshalBinary(data []byte) error {
	var pb internal.TimeToLiveSpec
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	s.unmarshal(&pb)
	return nil
}

// TimeToLiveInfo represents metadata about a time-to-live.
type TimeToLiveInfo struct {
	Name           string
	ReplicaN       int
	Duration       time.Duration
	RegionDuration time.Duration
	Regions        []RegionInfo
	Subscriptions  []SubscriptionInfo
}

// NewTimeToLiveInfo returns a new instance of TimeToLiveInfo
// with default replication and duration.
func NewTimeToLiveInfo(name string) *TimeToLiveInfo {
	return &TimeToLiveInfo{
		Name:     name,
		ReplicaN: DefaultTimeToLiveReplicaN,
		Duration: DefaultTimeToLiveDuration,
	}
}

// DefaultTimeToLiveInfo returns a new instance of TimeToLiveInfo
// with default name, replication, and duration.
func DefaultTimeToLiveInfo() *TimeToLiveInfo {
	return NewTimeToLiveInfo(DefaultTimeToLiveName)
}

// Apply applies a specification to the time-to-live info.
func (ttl *TimeToLiveInfo) Apply(spec *TimeToLiveSpec) *TimeToLiveInfo {
	t := &TimeToLiveInfo{
		Name:           ttl.Name,
		ReplicaN:       ttl.ReplicaN,
		Duration:       ttl.Duration,
		RegionDuration: ttl.RegionDuration,
	}
	if spec.Name != "" {
		t.Name = spec.Name
	}
	if spec.ReplicaN != nil {
		t.ReplicaN = *spec.ReplicaN
	}
	if spec.Duration != nil {
		t.Duration = *spec.Duration
	}
	t.RegionDuration = normalisedShardDuration(spec.RegionDuration, ttl.Duration)
	return t
}

// RegionByTimestamp returns the region in the time-to-live that contains the timestamp,
// or nil if no region matches.
func (ttl *TimeToLiveInfo) RegionByTimestamp(timestamp time.Time) *RegionInfo {
	for i := range ttl.Regions {
		sgi := &ttl.Regions[i]
		if sgi.Contains(timestamp) && !sgi.Deleted() && (!sgi.Truncated() || timestamp.Before(sgi.TruncatedAt)) {
			return &ttl.Regions[i]
		}
	}

	return nil
}

// ExpiredRegions returns the Regions which are considered expired, for the given time.
func (ttl *TimeToLiveInfo) ExpiredRegions(t time.Time) []*RegionInfo {
	var regions = make([]*RegionInfo, 0)
	for i := range ttl.Regions {
		if ttl.Regions[i].Deleted() {
			continue
		}
		if ttl.Duration != 0 && ttl.Regions[i].EndTime.Add(ttl.Duration).Before(t) {
			regions = append(regions, &ttl.Regions[i])
		}
	}
	return regions
}

// DeletedRegions returns the Regions which are marked as deleted.
func (ttl *TimeToLiveInfo) DeletedRegions() []*RegionInfo {
	var regions = make([]*RegionInfo, 0)
	for i := range ttl.Regions {
		if ttl.Regions[i].Deleted() {
			regions = append(regions, &ttl.Regions[i])
		}
	}
	return regions
}

// marshal serializes to a protobuf representation.
func (ttl *TimeToLiveInfo) marshal() *internal.TimeToLiveInfo {
	pb := &internal.TimeToLiveInfo{
		Name:           proto.String(ttl.Name),
		ReplicaN:       proto.Uint32(uint32(ttl.ReplicaN)),
		Duration:       proto.Int64(int64(ttl.Duration)),
		RegionDuration: proto.Int64(int64(ttl.RegionDuration)),
	}

	pb.Regions = make([]*internal.RegionInfo, len(ttl.Regions))
	for i, sgi := range ttl.Regions {
		pb.Regions[i] = sgi.marshal()
	}

	pb.Subscriptions = make([]*internal.SubscriptionInfo, len(ttl.Subscriptions))
	for i, sub := range ttl.Subscriptions {
		pb.Subscriptions[i] = sub.marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (ttl *TimeToLiveInfo) unmarshal(pb *internal.TimeToLiveInfo) {
	ttl.Name = pb.GetName()
	ttl.ReplicaN = int(pb.GetReplicaN())
	ttl.Duration = time.Duration(pb.GetDuration())
	ttl.RegionDuration = time.Duration(pb.GetRegionDuration())

	if len(pb.GetRegions()) > 0 {
		ttl.Regions = make([]RegionInfo, len(pb.GetRegions()))
		for i, x := range pb.GetRegions() {
			ttl.Regions[i].unmarshal(x)
		}
	}
	if len(pb.GetSubscriptions()) > 0 {
		ttl.Subscriptions = make([]SubscriptionInfo, len(pb.GetSubscriptions()))
		for i, x := range pb.GetSubscriptions() {
			ttl.Subscriptions[i].unmarshal(x)
		}
	}
}

// clone returns a deep copy of ttli.
func (ttl TimeToLiveInfo) clone() TimeToLiveInfo {
	other := ttl

	if ttl.Regions != nil {
		other.Regions = make([]RegionInfo, len(ttl.Regions))
		for i := range ttl.Regions {
			other.Regions[i] = ttl.Regions[i].clone()
		}
	}

	return other
}

// MarshalBinary encodes TimeToLiveInfo to a binary format.
func (ttl *TimeToLiveInfo) MarshalBinary() ([]byte, error) {
	return proto.Marshal(ttl.marshal())
}

// UnmarshalBinary decodes TimeToLiveInfo from a binary format.
func (ttl *TimeToLiveInfo) UnmarshalBinary(data []byte) error {
	var pb internal.TimeToLiveInfo
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	ttl.unmarshal(&pb)
	return nil
}

// regionDuration returns the default duration for a region based on a time-to-live duration.
func regionDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}

// normalisedShardDuration returns normalised shard duration based on a time-to-live duration.
func normalisedShardDuration(sgd, d time.Duration) time.Duration {
	// If it is zero, it likely wasn't specified, so we default to the region duration
	if sgd == 0 {
		return regionDuration(d)
	}
	// If it was specified, but it's less than the MinTimeToLiveDuration, then normalize
	// to the MinTimeToLiveDuration
	if sgd < MinTimeToLiveDuration {
		return regionDuration(MinTimeToLiveDuration)
	}
	return sgd
}

// RegionInfo represents metadata about a region. The DeletedAt field is important
// because it makes it clear that a Region has been marked as deleted, and allow the system
// to be sure that a Region is not simply missing. If the DeletedAt is set, the system can
// safely delete any associated shards.
type RegionInfo struct {
	ID          uint64
	StartTime   time.Time
	EndTime     time.Time
	DeletedAt   time.Time
	Shards      []ShardInfo
	TruncatedAt time.Time
}

// RegionInfos implements sort.Interface on []RegionInfo, based
// on the StartTime field.
type RegionInfos []RegionInfo

// Len implements sort.Interface.
func (a RegionInfos) Len() int { return len(a) }

// Swap implements sort.Interface.
func (a RegionInfos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface.
func (a RegionInfos) Less(i, j int) bool {
	iEnd := a[i].EndTime
	if a[i].Truncated() {
		iEnd = a[i].TruncatedAt
	}

	jEnd := a[j].EndTime
	if a[j].Truncated() {
		jEnd = a[j].TruncatedAt
	}

	if iEnd.Equal(jEnd) {
		return a[i].StartTime.Before(a[j].StartTime)
	}

	return iEnd.Before(jEnd)
}

// Contains returns true iif StartTime ≤ t < EndTime.
func (sgi *RegionInfo) Contains(t time.Time) bool {
	return !t.Before(sgi.StartTime) && t.Before(sgi.EndTime)
}

// Overlaps returns whether the region contains data for the time range between min and max
func (sgi *RegionInfo) Overlaps(min, max time.Time) bool {
	return !sgi.StartTime.After(max) && sgi.EndTime.After(min)
}

// Deleted returns whether this Region has been deleted.
func (sgi *RegionInfo) Deleted() bool {
	return !sgi.DeletedAt.IsZero()
}

// Truncated returns true if this Region has been truncated (no new writes).
func (sgi *RegionInfo) Truncated() bool {
	return !sgi.TruncatedAt.IsZero()
}

// clone returns a deep copy of sgi.
func (sgi RegionInfo) clone() RegionInfo {
	other := sgi

	if sgi.Shards != nil {
		other.Shards = make([]ShardInfo, len(sgi.Shards))
		for i := range sgi.Shards {
			other.Shards[i] = sgi.Shards[i].clone()
		}
	}

	return other
}

type hashIDer interface {
	HashID() uint64
}

// ShardFor returns the ShardInfo for a Point or other hashIDer.
func (sgi *RegionInfo) ShardFor(p hashIDer) ShardInfo {
	if len(sgi.Shards) == 1 {
		return sgi.Shards[0]
	}
	return sgi.Shards[p.HashID()%uint64(len(sgi.Shards))]
}

// marshal serializes to a protobuf representation.
func (sgi *RegionInfo) marshal() *internal.RegionInfo {
	pb := &internal.RegionInfo{
		ID:        proto.Uint64(sgi.ID),
		StartTime: proto.Int64(MarshalTime(sgi.StartTime)),
		EndTime:   proto.Int64(MarshalTime(sgi.EndTime)),
		DeletedAt: proto.Int64(MarshalTime(sgi.DeletedAt)),
	}

	if !sgi.TruncatedAt.IsZero() {
		pb.TruncatedAt = proto.Int64(MarshalTime(sgi.TruncatedAt))
	}

	pb.Shards = make([]*internal.ShardInfo, len(sgi.Shards))
	for i := range sgi.Shards {
		pb.Shards[i] = sgi.Shards[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (sgi *RegionInfo) unmarshal(pb *internal.RegionInfo) {
	sgi.ID = pb.GetID()
	if i := pb.GetStartTime(); i == 0 {
		sgi.StartTime = time.Unix(0, 0).UTC()
	} else {
		sgi.StartTime = UnmarshalTime(i)
	}
	if i := pb.GetEndTime(); i == 0 {
		sgi.EndTime = time.Unix(0, 0).UTC()
	} else {
		sgi.EndTime = UnmarshalTime(i)
	}
	sgi.DeletedAt = UnmarshalTime(pb.GetDeletedAt())

	if pb != nil && pb.TruncatedAt != nil {
		sgi.TruncatedAt = UnmarshalTime(pb.GetTruncatedAt())
	}

	if len(pb.GetShards()) > 0 {
		sgi.Shards = make([]ShardInfo, len(pb.GetShards()))
		for i, x := range pb.GetShards() {
			sgi.Shards[i].unmarshal(x)
		}
	}
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID     uint64
	Owners []ShardOwner
}

// OwnedBy determines whether the shard's owner IDs includes nodeID.
func (si ShardInfo) OwnedBy(nodeID uint64) bool {
	for _, so := range si.Owners {
		if so.NodeID == nodeID {
			return true
		}
	}
	return false
}

// clone returns a deep copy of si.
func (si ShardInfo) clone() ShardInfo {
	other := si

	if si.Owners != nil {
		other.Owners = make([]ShardOwner, len(si.Owners))
		for i := range si.Owners {
			other.Owners[i] = si.Owners[i].clone()
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (si ShardInfo) marshal() *internal.ShardInfo {
	pb := &internal.ShardInfo{
		ID: proto.Uint64(si.ID),
	}

	pb.Owners = make([]*internal.ShardOwner, len(si.Owners))
	for i := range si.Owners {
		pb.Owners[i] = si.Owners[i].marshal()
	}

	return pb
}

// UnmarshalBinary decodes the object from a binary format.
func (si *ShardInfo) UnmarshalBinary(buf []byte) error {
	var pb internal.ShardInfo
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	si.unmarshal(&pb)
	return nil
}

// unmarshal deserializes from a protobuf representation.
func (si *ShardInfo) unmarshal(pb *internal.ShardInfo) {
	si.ID = pb.GetID()

	// If deprecated "OwnerIDs" exists then convert it to "Owners" format.
	if len(pb.GetOwnerIDs()) > 0 {
		si.Owners = make([]ShardOwner, len(pb.GetOwnerIDs()))
		for i, x := range pb.GetOwnerIDs() {
			si.Owners[i].unmarshal(&internal.ShardOwner{
				NodeID: proto.Uint64(x),
			})
		}
	} else if len(pb.GetOwners()) > 0 {
		si.Owners = make([]ShardOwner, len(pb.GetOwners()))
		for i, x := range pb.GetOwners() {
			si.Owners[i].unmarshal(x)
		}
	}
}

// SubscriptionInfo holds the subscription information.
type SubscriptionInfo struct {
	Name         string
	Mode         string
	Destinations []string
}

// marshal serializes to a protobuf representation.
func (si SubscriptionInfo) marshal() *internal.SubscriptionInfo {
	pb := &internal.SubscriptionInfo{
		Name: proto.String(si.Name),
		Mode: proto.String(si.Mode),
	}

	pb.Destinations = make([]string, len(si.Destinations))
	for i := range si.Destinations {
		pb.Destinations[i] = si.Destinations[i]
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (si *SubscriptionInfo) unmarshal(pb *internal.SubscriptionInfo) {
	si.Name = pb.GetName()
	si.Mode = pb.GetMode()

	if len(pb.GetDestinations()) > 0 {
		si.Destinations = make([]string, len(pb.GetDestinations()))
		copy(si.Destinations, pb.GetDestinations())
	}
}

// ShardOwner represents a node that owns a shard.
type ShardOwner struct {
	NodeID uint64 // if NodeID is 0 , the Shard is a local shard
}

// clone returns a deep copy of so.
func (so ShardOwner) clone() ShardOwner {
	return so
}

// marshal serializes to a protobuf representation.
func (so ShardOwner) marshal() *internal.ShardOwner {
	return &internal.ShardOwner{
		NodeID: proto.Uint64(so.NodeID),
	}
}

// unmarshal deserializes from a protobuf representation.
func (so *ShardOwner) unmarshal(pb *internal.ShardOwner) {
	so.NodeID = pb.GetNodeID()
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	Name  string
	Query string
}

// clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) clone() ContinuousQueryInfo { return cqi }

// marshal serializes to a protobuf representation.
func (cqi ContinuousQueryInfo) marshal() *internal.ContinuousQueryInfo {
	return &internal.ContinuousQueryInfo{
		Name:  proto.String(cqi.Name),
		Query: proto.String(cqi.Query),
	}
}

// unmarshal deserializes from a protobuf representation.
func (cqi *ContinuousQueryInfo) unmarshal(pb *internal.ContinuousQueryInfo) {
	cqi.Name = pb.GetName()
	cqi.Query = pb.GetQuery()
}

// UserInfo represents metadata about a user in the system.
type UserInfo struct {
	// User's name.
	Name string

	// Hashed password.
	Hash string

	// Whether the user is an admin, i.e. allowed to do everything.
	Admin bool

	// Map of database name to granted privilege.
	Privileges map[string]cnosql.Privilege
}

type User interface {
	ID() string
}

func (u *UserInfo) ID() string {
	return u.Name
}

// IsOpen is a method on FineAuthorizer to indicate all fine auth is permitted and short circuit some checks.
func (u *UserInfo) IsOpen() bool {
	return true
}

// clone returns a deep copy of si.
func (u UserInfo) clone() UserInfo {
	other := u

	if u.Privileges != nil {
		other.Privileges = make(map[string]cnosql.Privilege)
		for k, v := range u.Privileges {
			other.Privileges[k] = v
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (u UserInfo) marshal() *internal.UserInfo {
	pb := &internal.UserInfo{
		Name:  proto.String(u.Name),
		Hash:  proto.String(u.Hash),
		Admin: proto.Bool(u.Admin),
	}

	for database, privilege := range u.Privileges {
		pb.Privileges = append(pb.Privileges, &internal.UserPrivilege{
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(privilege)),
		})
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (u *UserInfo) unmarshal(pb *internal.UserInfo) {
	u.Name = pb.GetName()
	u.Hash = pb.GetHash()
	u.Admin = pb.GetAdmin()

	u.Privileges = make(map[string]cnosql.Privilege)
	for _, p := range pb.GetPrivileges() {
		u.Privileges[p.GetDatabase()] = cnosql.Privilege(p.GetPrivilege())
	}
}

// MarshalTime converts t to nanoseconds since epoch. A zero time returns 0.
func MarshalTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// UnmarshalTime converts nanoseconds since epoch to time.
// A zero value returns a zero time.
func UnmarshalTime(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

// ValidName checks to see if the given name can would be valid for DB/TTL name
func ValidName(name string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`)
}
