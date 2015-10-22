package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"os"
)

// Remove removes the named file or directory.
func (c *Client) Remove(name string) error {
	_, err := c.getFileInfo(name)
	if err != nil {
		return &os.PathError{"remove", name, err}
	}

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(true),
	}
	resp := &hdfs.DeleteResponseProto{}

	err = c.namenode.Execute("delete", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"remove", name, err}
	} else if resp.Result == nil {
		return &os.PathError{
			"remove",
			name,
			errors.New("Unexpected empty response to 'delete' rpc call"),
		}
	}

	return nil
}
