package hdf5

// #include "hdf5.h"
// #include "hdf5_hl.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"

	"reflect"
	"runtime"
	"unsafe"
)

type Table struct {
	Location
}

func newPacketTable(id C.hid_t) *Table {
	t := &Table{Location{Identifier{id}}}
	runtime.SetFinalizer(t, (*Table).finalizer)
	return t
}

func (t *Table) finalizer() {
	err := t.Close()
	if err != nil {
		panic(fmt.Sprintf("error closing packet table: %s", err))
	}
}

// Close closes an open packet table.
func (t *Table) Close() error {
	if t.id > 0 {
		err := h5err(C.H5PTclose(t.id))
		if err != nil {
			t.id = 0
		}
		return err
	}
	return nil
}

// IsValid returns whether or not an indentifier points to a packet table.
func (t *Table) IsValid() bool {
	o := int(C.H5PTis_valid(t.id))
	if o > 0 {
		return true
	}
	return false
}

func (t *Table) Id() int {
	return int(t.id)
}

// ReadPackets reads a number of packets from a packet table.
func (t *Table) ReadPackets(start, nrecords int, data interface{}) error {
	c_start := C.hsize_t(start)
	c_nrecords := C.size_t(nrecords)
	rt := reflect.TypeOf(data)
	rv := reflect.ValueOf(data)
	c_data := unsafe.Pointer(nil)
	switch rt.Kind() {
	case reflect.Array:
		if rv.Cap() < nrecords {
			panic(fmt.Sprintf("not enough capacity in array (cap=%d)", rv.Cap()))
		}
		c_data = unsafe.Pointer(rv.Index(0).UnsafeAddr())

	default:
		panic(fmt.Sprintf("unhandled kind (%s), need array", rt.Kind()))
	}
	err := C.H5PTread_packets(t.id, c_start, c_nrecords, c_data)
	return h5err(err)
}

// Append appends packets to the end of a packet table.
func (t *Table) Append(data interface{}) error {
	rt := reflect.TypeOf(data)
	v := reflect.ValueOf(data)
	c_nrecords := C.size_t(0)
	c_data := unsafe.Pointer(nil)

	switch rt.Kind() {

	case reflect.Array:
		c_nrecords = C.size_t(v.Len())
		c_data = unsafe.Pointer(v.UnsafeAddr())

	case reflect.String:
		c_nrecords = C.size_t(v.Len())
		str := (*reflect.StringHeader)(unsafe.Pointer(v.UnsafeAddr()))
		c_data = unsafe.Pointer(str.Data)

	case reflect.Ptr:
		c_nrecords = C.size_t(1)
		c_data = unsafe.Pointer(v.Elem().UnsafeAddr())

	default:
		c_nrecords = C.size_t(1)
		c_data = unsafe.Pointer(v.UnsafeAddr())
	}

	err := C.H5PTappend(t.id, c_nrecords, c_data)
	return h5err(err)
}

// Next reads packets from a packet table starting at the current index.
func (t *Table) Next(data interface{}) error {
	rt := reflect.TypeOf(data)
	rv := reflect.ValueOf(data)
	n := C.size_t(0)
	cdata := unsafe.Pointer(nil)
	switch rt.Kind() {
	case reflect.Array:
		if rv.Cap() <= 0 {
			panic(fmt.Sprintf("not enough capacity in array (cap=%d)", rv.Cap()))
		}
		cdata = unsafe.Pointer(rv.Index(0).UnsafeAddr())
		n = C.size_t(rv.Cap())
	default:
		panic(fmt.Sprintf("unsupported kind (%s), need slice or array", rt.Kind()))
	}
	err := C.H5PTget_next(t.id, n, cdata)
	return h5err(err)
}

// NumPackets returns the number of packets in a packet table.
func (t *Table) NumPackets() (int, error) {
	c_nrecords := C.hsize_t(0)
	err := C.H5PTget_num_packets(t.id, &c_nrecords)
	return int(c_nrecords), h5err(err)
}

// CreateIndex resets a packet table's index to the first packet.
func (t *Table) CreateIndex() error {
	err := C.H5PTcreate_index(t.id)
	return h5err(err)
}

// SetIndex sets a packet table's index.
func (t *Table) SetIndex(index int) error {
	c_idx := C.hsize_t(index)
	err := C.H5PTset_index(t.id, c_idx)
	return h5err(err)
}

// Type returns an identifier for a copy of the datatype for a dataset.
func (t *Table) Type() (*Datatype, error) {
	hid := C.H5Dget_type(t.id)
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	return copyDatatype(hid)
}

func createTable(id C.hid_t, name string, dtype *Datatype, chunkSize, compression int) (*Table, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	chunk := C.hsize_t(chunkSize)
	compr := C.int(compression)
	hid := C.H5PTcreate_fl(id, c_name, dtype.id, chunk, compr)
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	table := newPacketTable(hid)
	return table, err
}

func createTableFrom(id C.hid_t, name string, dtype interface{}, chunkSize, compression int) (*Table, error) {
	var err error
	switch dt := dtype.(type) {
	case reflect.Type:
		if hdfDtype, err := NewDataTypeFromType(dt); err == nil {
			return createTable(id, name, hdfDtype, chunkSize, compression)
		}
	case *Datatype:
		return createTable(id, name, dt, chunkSize, compression)
	default:
		if hdfDtype, err := NewDataTypeFromType(reflect.TypeOf(dtype)); err == nil {
			return createTable(id, name, hdfDtype, chunkSize, compression)
		}
	}
	return nil, err
}

func openTable(id C.hid_t, name string) (*Table, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	hid := C.H5PTopen(id, c_name)
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	table := newPacketTable(hid)
	return table, err
}
