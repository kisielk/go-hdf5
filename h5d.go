package hdf5

// #include "hdf5.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"

	"reflect"
	"runtime"
	"unsafe"
)

type Dataset struct {
	Location
}

func newDataset(id C.hid_t) *Dataset {
	d := &Dataset{Location{Identifier{id}}}
	runtime.SetFinalizer(d, (*Dataset).finalizer)
	return d
}

func createDataset(id C.hid_t, name string, dtype *Datatype, dspace *Dataspace, dcpl *PropList) (*Dataset, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	hid := C.H5Dcreate2(id, c_name, dtype.id, dspace.id, P_DEFAULT.id, dcpl.id, P_DEFAULT.id)
	if err := h5err(C.herr_t(int(hid))); err != nil {
		return nil, err
	}
	return newDataset(hid), nil
}

func (s *Dataset) finalizer() {
	err := s.Close()
	if err != nil {
		panic(fmt.Sprintf("error closing dset: %s", err))
	}
}

// Close releases and terminates access to a dataset.
func (s *Dataset) Close() error {
	if s.id > 0 {
		err := C.H5Dclose(s.id)
		s.id = 0
		return h5err(err)
	}
	return nil
}

// Space returns an identifier for a copy of the dataspace for a dataset.
func (s *Dataset) Space() *Dataspace {
	hid := C.H5Dget_space(s.id)
	if int(hid) > 0 {
		return newDataspace(hid)
	}
	return nil
}

// Read reads raw data from a dataset into a buffer.
func (s *Dataset) Read(data interface{}) error {
	dtype, err := s.Datatype()
	if err != nil {
		return err
	}

	var addr uintptr
	v := reflect.ValueOf(data)

	switch v.Kind() {

	case reflect.Array:
		addr = v.UnsafeAddr()

	case reflect.String:
		str := (*reflect.StringHeader)(unsafe.Pointer(v.UnsafeAddr()))
		addr = str.Data

	case reflect.Ptr:
		addr = v.Pointer()

	default:
		addr = v.UnsafeAddr()
	}

	rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(addr))
	err = h5err(rc)
	return err
}

// Write writes raw data from a buffer to a dataset.
func (s *Dataset) Write(data interface{}) error {
	dtype, err := s.Datatype()
	if err != nil {
		return err
	}

	var addr uintptr
	v := reflect.ValueOf(data)
	switch v.Kind() {

	case reflect.Array:
		addr = v.UnsafeAddr()

	case reflect.String:
		str := (*reflect.StringHeader)(unsafe.Pointer(v.UnsafeAddr()))
		addr = str.Data

	case reflect.Ptr:
		addr = v.Pointer()

	default:
		addr = v.Pointer()
	}

	rc := C.H5Dwrite(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(addr))
	err = h5err(rc)
	return err
}

// Datatype returns the HDF5 Datatype of the Dataset
func (s *Dataset) Datatype() (*Datatype, error) {
	dtype_id := C.H5Dget_type(s.id)
	if dtype_id < 0 {
		return nil, fmt.Errorf("couldn't open Datatype from Dataset %q", s.Name())
	}
	return copyDatatype(dtype_id)
}
