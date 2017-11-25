// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*
Package pconn implements a generic persistent connection manager.

Persistent Connection Manager Overview

Persistent Connection Manger handles the responsibility of managing connection
attempts to persistent peers. It offers the ability to reliably cancel outbound
requests, simplifying the process of selecting a single successful connection
from multiple pending connection requests.
*/
package pconn
