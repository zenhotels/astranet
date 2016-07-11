package skykiss

import "io"

type IOLoop struct {
       io.Reader
       io.Writer
}