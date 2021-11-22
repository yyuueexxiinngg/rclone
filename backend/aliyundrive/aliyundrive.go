package aliyundrive

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/K265/aliyundrive-go/pkg/aliyun/drive"
	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	maxFileNameLength = 1024
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "aliyundrive",
		Description: "Aliyun Drive",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "refresh_token",
			Help: `Refresh Token used to access aliyun drive
you can find this by executing localStorage.token in the Chrome console`,
			Required: true,
		}, {
			Name:    "is_album",
			Help:    "for aliyun drive albums",
			Default: false,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	RefreshToken string               `config:"refresh_token"`
	IsAlbum      bool                 `config:"is_album"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote aliyundrive server
type Fs struct {
	name     string             // name of this remote
	root     string             // the path we are working on if any
	opt      Options            // parsed config options
	ci       *fs.ConfigInfo     // global config
	features *fs.Features       // optional features
	srv      drive.Fs           // the connection to the aliyundrive api
	dirCache *dircache.DirCache // Map of directory path to directory id
}

// absPath return absolute path of remote
func (f *Fs) absPath(remote string) string {
	return path.Join("/", f.root, remote)
}

// Object describes a aliyundrive object
type Object struct {
	fs     *Fs    // what this object is part of
	remote string // The remote path
	node   *drive.Node
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if f == nil {
		return f, err
	}

	rootNode, err2 := f.srv.GetByPath(ctx, "", drive.FolderKind)
	if err2 != nil {
		return nil, err2
	}

	f.dirCache = dircache.New(f.root, rootNode.NodeId, f)
	return f, err
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	nodes, err := f.srv.List(ctx, pathID)
	if err != nil {
		return "", false, err
	}

	for _, node := range nodes {
		if node.IsDirectory() && node.Name == leaf {
			return node.NodeId, true, nil
		}
	}

	return "", false, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	leaf = f.opt.Enc.FromStandardName(leaf)
	nodeId, err := f.srv.CreateFolder(ctx, pathID, leaf)
	if err != nil {
		return "", err
	}

	return nodeId, nil
}

// newFs partially constructs Fs from the path
func newFs(ctx context.Context, name, root string, m configmap.Mapper) (*Fs, error) {
	// Parse conf into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	ci := fs.GetConfig(ctx)

	conf := &drive.Config{
		RefreshToken: opt.RefreshToken,
		IsAlbum:      opt.IsAlbum,
		HttpClient:   fshttp.NewClient(ctx),
	}

	srv, err := drive.NewFs(ctx, conf)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		ci:   ci,
		srv:  srv,
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	if f.root == "/" {
		return f, nil
	}
	// refer from https://github.com/rclone/rclone/blob/v1.56.0/backend/local/local.go#L286
	// Check to see if this points to a file
	node, err := f.srv.GetByPath(ctx, f.root, drive.AnyKind)
	if err == nil && !node.IsDirectory() {
		// It is a file, so use the parent as the root
		f.root = filepath.Dir(f.root)
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("aliyundrive root '%s'", f.root)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

func getNodeModTime(node *drive.Node) time.Time {
	t, _ := node.GetTime()
	return t
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	directoryId, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	nodes, err := f.srv.List(ctx, directoryId)
	if err != nil {
		return nil, err
	}

	for _, _node := range nodes {
		node := _node
		remote := path.Join(dir, node.Name)
		if node.IsDirectory() {
			// cache the directory ID for later lookups
			f.dirCache.Put(remote, node.NodeId)
			entries = append(entries, fs.NewDir(remote, getNodeModTime(&node)))
		} else {
			o := &Object{
				fs:     f,
				remote: remote,
				node:   &node,
			}
			entries = append(entries, o)
		}
	}
	return entries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, directoryID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		fs.Debugf(f, "NewObject dirCache.FindPath: %v", directoryID)
		return nil, fs.ErrorObjectNotFound
	}

	nodes, err := f.srv.List(ctx, directoryID)
	if err != nil {
		return nil, err
	}
	for _, _node := range nodes {
		node := _node
		if node.Name == leaf && !node.IsDirectory() {
			return &Object{
				fs:     f,
				remote: remote,
				node:   &node,
			}, nil
		}
	}

	return nil, fs.ErrorObjectNotFound
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	existingObj, err := f.NewObject(ctx, src.Remote())
	switch err {
	case nil:
		if o, ok := existingObj.(*Object); ok {
			if err := f.srv.Remove(ctx, o.node.NodeId); err != nil {
				return nil, err
			}
		}
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		// Not found so create it
		o := &Object{
			fs:     f,
			remote: src.Remote(),
		}
		return o, o.Update(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	directoryId, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	err = f.srv.Remove(ctx, directoryId)
	f.dirCache.FlushDir(dir)
	return err
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Errorf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}

	dstParentId, err := f.dirCache.FindDir(ctx, remote, false)
	if err != nil {
		fs.Errorf(nil, "Copy dirCache.FindDir: %v", err)
		return nil, fs.ErrorCantCopy
	}

	dstName := path.Base(remote)
	nodeId, err := f.srv.Copy(ctx, srcObj.node.NodeId, dstParentId, dstName)
	if err != nil {
		fs.Errorf(nil, "Copy copy: %v", err)
		return nil, fs.ErrorCantCopy
	}

	node, err := f.srv.Get(ctx, nodeId)
	if err != nil {
		return nil, fs.ErrorCantCopy
	}

	dstObj := &Object{
		fs:     f,
		remote: remote,
		node:   node,
	}
	return dstObj, nil
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Errorf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	leaf, dstParentId, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		fs.Errorf(nil, "Move dirCache.FindPath: %v", err)
		return nil, fs.ErrorCantMove
	}

	leaf = f.opt.Enc.FromStandardName(leaf)
	nodeId, err := f.srv.Move(ctx, srcObj.node.NodeId, dstParentId, leaf)
	if err != nil {
		fs.Errorf(nil, "Move move: %v", err)
		return nil, fs.ErrorCantMove
	}

	node, err := f.srv.Get(ctx, nodeId)
	if err != nil {
		fs.Errorf(nil, "Move get: %v", err)
		return nil, fs.ErrorCantMove
	}

	dstObj := &Object{
		fs:     f,
		remote: remote,
		node:   node,
	}
	return dstObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Errorf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, _, _, dstDirectoryID, _, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		fs.Errorf(nil, "DirMove dirCache.DirMove: %v", err)
		return fs.ErrorCantDirMove
	}

	dstPath := f.absPath(dstRemote)
	dstName := path.Base(dstPath)
	_, err = f.srv.Move(ctx, srcID, dstDirectoryID, dstName)
	if err != nil {
		fs.Errorf(nil, "DirMove move: %v", err)
		return fs.ErrorCantDirMove
	}

	return nil
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(context.Context) time.Time {
	return getNodeModTime(o.node)
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.node.Size
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.SHA1 {
		return "", hash.ErrUnsupported
	}

	return strings.ToLower(o.node.Hash), nil
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.Size())
	headers := map[string]string{}
	fs.OpenOptionAddHeaders(options, headers)
	return o.fs.srv.Open(ctx, o.node.NodeId, headers)
}

const rapidUploadLimit = 50 * 1024 * 1024 // 50 MB

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	sha1Code := ""
	proofCode := ""
	fileSize := src.Size()
	localObj, ok := src.(*local.Object)
	if ok && fileSize > rapidUploadLimit {
		localRoot := localObj.Fs().Root()[4:]
		localRemote := localObj.Remote()
		localPath := path.Join(localRoot, localRemote)
		fd, err := os.Open(localPath)
		if err == nil {
			_, sha1Code, _ = drive.CalcSha1(fd)
			_, proofCode, _ = o.fs.srv.CalcProof(fileSize, fd)
			_ = fd.Close()
		}
	}

	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, o.Remote(), true)
	if err != nil {
		return err
	}

	nodeId, err := o.fs.srv.CreateFileWithProof(ctx, directoryID, leaf, src.Size(), in, sha1Code, proofCode)
	if err != nil {
		return err
	}

	node, err := o.fs.srv.Get(ctx, nodeId)
	if err != nil {
		return err
	}

	o.node = node
	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.srv.Remove(ctx, o.node.NodeId)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs       = (*Fs)(nil)
	_ fs.Copier   = (*Fs)(nil)
	_ fs.Mover    = (*Fs)(nil)
	_ fs.DirMover = (*Fs)(nil)
	_ fs.Object   = (*Object)(nil)
)
