package test

import (
	"archive/tar"
	"context"
	"io"
	"os"

	"github.com/restic/restic/internal/restic"
)

type tarRepo struct {
	restic.Repository
	io.Reader
}

func (t tarRepo) LoadBlob(ctx context.Context, tpe restic.BlobType, id restic.ID, b []byte) ([]byte, error) {
	_, err := io.ReadFull(t, b)
	return b, err
}

type tarNode struct {
	header *tar.Header
}

func (t tarNode) Node() *restic.Node {
	hdr := t.header
	var tpe string
	switch hdr.Typeflag {
	case tar.TypeReg:
		tpe = "file"
	case tar.TypeDir:
		tpe = "dir"
	case tar.TypeLink:
		tpe = "symlink"
	case tar.TypeSymlink:
		tpe = "symlink"
	case tar.TypeChar:
		tpe = "chardev"
	case tar.TypeBlock:
		tpe = "dev"
	case tar.TypeFifo:
		tpe = "fifo"
	}

	return &restic.Node{Name: hdr.Name,
		Type:       tpe,
		Mode:       os.FileMode(hdr.Mode),
		ModTime:    hdr.ModTime,
		AccessTime: hdr.AccessTime,
		ChangeTime: hdr.ChangeTime,
		UID:        uint32(hdr.Uid),
		GID:        uint32(hdr.Gid),
		User:       hdr.Uname,
		Group:      hdr.Gname,
		Size:       uint64(hdr.Size),
		LinkTarget: hdr.Linkname,
	}
}

func extactTar(ctx context.Context, rd io.Reader, outputDir string) error {
	tarReader := tar.NewReader(rd)

	for {
		header, err := tarReader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		t := tarNode{header: header}
		repo := tarRepo{Reader: tarReader}
		err = t.Node().CreateAt(ctx, outputDir, repo)
		if err != nil {
			return err
		}
		err = t.Node().RestoreMetadata(outputDir)
		if err != nil {
			return err
		}
	}
	return nil
}
