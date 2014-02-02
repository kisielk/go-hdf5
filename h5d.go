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
	dtype, err := dtype.Copy() // For safety
	if err != nil {
		return nil, err
	}
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

// Read reads data from a Dataset. It dynamically determines what kind of
// return type to produce depending on the Datatype and the Dataspace of the
// Dataset. If the Dataspace is scalar, then Read will return a value.
// Otherwise, Read will return a slice of values. If the Datatype's TypeClass
// is T_COMPOUND, then Read will fail. Use ReadInto instead.
//
// Example:
//     data := dset.Read()
//     intData, ok := data.([]int64)
//     if ok { ... }
func (s *Dataset) Read() (interface{}, error) {
	dtype, err := s.Datatype()
	if err != nil {
		return nil, err
	}
	// TODO generically read T_COMPOUND data into some kind of map?
	if dtype.Class() == T_COMPOUND {
		return nil, fmt.Errorf("cannot read T_COMPOUND with this function")
	}

	var rval reflect.Value
	var addr uintptr

	space := s.Space()
	goType := dtype.GoType()

	switch space.SimpleExtentNDims() {
	case 0:
		// Unfortunately scalar datasets are kind of a pain. There is a case
		// for each type.
		switch goType {
		case goStringType:
			// FIXME special case to handle string conversion by copying
			buf := make([]*C.char, 1, 1)
			defer C.free(unsafe.Pointer(buf[0]))
			// H5DRead allocates the memory for buf for us, but we have to free
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf[0]))

			if err = h5err(rc); err != nil {
				return nil, err
			}
			rval := C.GoString(buf[0])
			return rval, err
		case goInt8Type:
			var buf C.char
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return int8(buf), err
		case goInt16Type:
			var buf C.short
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return int16(buf), err
		case goInt32Type:
			var buf C.int
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return int32(buf), err
		case goInt64Type:
			var buf C.longlong
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return int64(buf), err
		case goUint8Type:
			var buf C.uchar
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return uint8(buf), err
		case goUint16Type:
			var buf C.ushort
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return uint16(buf), err
		case goUint32Type:
			var buf C.uint
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return uint32(buf), err
		case goUint64Type:
			var buf C.ulonglong
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return uint64(buf), err
		case goFloat32Type:
			var buf C.float
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return float32(buf), err
		case goFloat64Type:
			var buf C.double
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&buf))
			err = h5err(rc)
			return float64(buf), err
		default:
			rval = reflect.New(goType)
			addr = rval.UnsafeAddr()
		}
	case 1:
		dataLen := space.SimpleExtentNPoints()
		switch goType {
		case goStringType:
			// FIXME special case to handle string conversion by copying
			buf := make([]*C.char, dataLen, dataLen)
			rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(&(buf[0])))

			if err = h5err(rc); err != nil {
				return nil, err
			}

			rval := make([]string, dataLen, dataLen)
			for i := range buf {
				rval[i] = C.GoString(buf[i])
				defer C.free(unsafe.Pointer(buf[i]))
			}
			return rval, err

		default:
			slice := reflect.SliceOf(goType)
			rval = reflect.MakeSlice(slice, dataLen, dataLen)
			addr = rval.Pointer()
		}
	default:
		//TODO
		panic("no support for higher-dimensional spaces yet... stay tuned")

	}

	rc := C.H5Dread(s.id, dtype.id, 0, 0, 0, unsafe.Pointer(addr))
	err = h5err(rc)
	return rval.Interface(), err
}

// ReadInto is similar to Read, but uses spec to determine the return type.
// This method is useful for deserializing structs.
func (s *Dataset) ReadInto(spec reflect.Type) (interface{}, error) {
	// TODO
	return nil, nil
}

func cStringSlice(data reflect.Value) []*C.char {
	dataLen := data.Len()
	cArray := make([]*C.char, dataLen, dataLen)
	for i := 0; i < dataLen; i++ {
		gostr := data.Index(i).String()
		cstr := C.CString(gostr)
		cArray[i] = cstr
	}
	return cArray
}

func freeCStringSlice(data []*C.char) {
	for i := range data {
		C.free(unsafe.Pointer(data[i]))
	}
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

	case reflect.Slice, reflect.Ptr:
		switch v.Type().Elem().Kind() {
		case reflect.String:
			// FIXME: Write a C function to avoid copying the Go string sequence
			// into a C char* sequence?
			cStrings := cStringSlice(v)
			defer freeCStringSlice(cStrings)

			addr = uintptr(unsafe.Pointer(&cStrings[0]))
		default:
			addr = v.Pointer()
		}

	case reflect.String:
		str := v.String()

		cstr := make([]*C.char, 1, 1)
		cstr[0] = C.CString(str)
		defer C.free(unsafe.Pointer(cstr[0]))

		addr = uintptr(unsafe.Pointer(&cstr[0]))

	case reflect.Int8:
		val := int8(v.Int())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Int16:
		val := int16(v.Int())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Int32:
		val := int32(v.Int())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Int64:
		val := v.Int()
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Uint8:
		val := uint8(v.Uint())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Uint16:
		val := uint16(v.Uint())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Uint32:
		val := uint32(v.Uint())
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Uint64:
		val := v.Uint()
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Float32:
		val := float32(v.Float()) // Is this conversion harmful?
		addr = uintptr(unsafe.Pointer(&val))

	case reflect.Float64:
		val := v.Float()
		addr = uintptr(unsafe.Pointer(&val))

	default:
		return fmt.Errorf(
			"couldn't read from this datatype, try a pointer or slice instead.")
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
