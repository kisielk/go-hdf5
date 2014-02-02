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

type Datatype struct {
	Location
}

type TypeClass C.H5T_class_t

const (
	T_NO_CLASS  TypeClass = -1 // Error
	T_INTEGER   TypeClass = 0  // integer types
	T_FLOAT     TypeClass = 1  // floating-point types
	T_TIME      TypeClass = 2  // date and time types
	T_STRING    TypeClass = 3  // character string types
	T_BITFIELD  TypeClass = 4  // bit field types
	T_OPAQUE    TypeClass = 5  // opaque types
	T_COMPOUND  TypeClass = 6  // compound types
	T_REFERENCE TypeClass = 7  // reference types
	T_ENUM      TypeClass = 8  // enumeration types
	T_VLEN      TypeClass = 9  // variable-length types
	T_ARRAY     TypeClass = 10 // array types
	T_NCLASSES  TypeClass = 11 // nbr of classes -- MUST BE LAST
)

// list of go types
var (
	goStringType reflect.Type = reflect.TypeOf(string(""))
	goIntType    reflect.Type = reflect.TypeOf(int(0))
	goInt8Type   reflect.Type = reflect.TypeOf(int8(0))
	goInt16Type  reflect.Type = reflect.TypeOf(int16(0))
	goInt32Type  reflect.Type = reflect.TypeOf(int32(0))
	goInt64Type  reflect.Type = reflect.TypeOf(int64(0))
	goUintType   reflect.Type = reflect.TypeOf(uint(0))
	goUint8Type  reflect.Type = reflect.TypeOf(uint8(0))
	goUint16Type reflect.Type = reflect.TypeOf(uint16(0))
	goUint32Type reflect.Type = reflect.TypeOf(uint32(0))
	goUint64Type reflect.Type = reflect.TypeOf(uint64(0))

	goFloat32Type reflect.Type = reflect.TypeOf(float32(0))
	goFloat64Type reflect.Type = reflect.TypeOf(float64(0))

	goArrayType reflect.Type = reflect.TypeOf([1]int{0})
	goSliceType reflect.Type = reflect.TypeOf([]int{0})

	goStructType reflect.Type = reflect.TypeOf(struct{}{})

	goPtrType reflect.Type = reflect.PtrTo(goIntType)

	goBoolType reflect.Type = reflect.TypeOf(true)
)

type typeSet map[TypeClass]struct{}

var (
	parametricTypes typeSet = typeSet{
		// Only these types can be used with CreateDatatype
		T_COMPOUND: struct{}{},
		T_ENUM:     struct{}{},
		T_OPAQUE:   struct{}{},
		T_STRING:   struct{}{},
	}
)

func OpenDatatype(c CommonFG, name string, tapl_id int) (*Datatype, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	id := C.H5Topen2(C.hid_t(c.id), c_name, C.hid_t(tapl_id))
	err := h5err(C.herr_t(id))
	if err != nil {
		return nil, err
	}
	dt := &Datatype{Location{Identifier{id}}}
	runtime.SetFinalizer(dt, (*Datatype).finalizer)
	return dt, err
}

func NewDatatype(id C.hid_t) *Datatype {
	t := &Datatype{Location{Identifier{id}}}
	runtime.SetFinalizer(t, (*Datatype).finalizer)
	return t
}

// CreateDatatype creates a new datatype.
// class must be T_COMPUND, T_OPAQUE, T_ENUM or T_STRING.
// size is the size of the new datatype in bytes.
func CreateDatatype(class TypeClass, size int) (*Datatype, error) {
	_, ok := parametricTypes[class]
	if !ok {
		return nil, fmt.Errorf(
			"invalid TypeClass, want %v, %v, %v or %v, got %v",
			T_COMPOUND, T_OPAQUE, T_STRING, T_ENUM)
	}

	hid := C.H5Tcreate(C.H5T_class_t(class), C.size_t(size))
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	return NewDatatype(hid), nil
}

func (t *Datatype) finalizer() {
	err := t.Close()
	if err != nil {
		panic(fmt.Sprintf("error closing datatype: %s", err))
	}
}

// GoType returns the reflect.Type associated with the Datatype's TypeClass
func (t *Datatype) GoType() reflect.Type {
	switch t.Class() {
	case T_NO_CLASS:
		return nil
	case T_INTEGER, T_ENUM:
		sign := int(C.H5Tget_sign(t.id))
		if sign < 0 {
			panic("Datatype class is T_INTEGER, but couldn't determine sign")
		}
		switch t.Size() {
		case 1:
			if sign > 0 {
				return goInt8Type
			} else {
				return goUint8Type
			}
		case 2:
			if sign > 0 {
				return goInt16Type
			} else {
				return goUint16Type
			}
		case 4:
			if sign > 0 {
				return goInt32Type
			} else {
				return goUint32Type
			}
		case 8:
			if sign > 0 {
				return goInt64Type
			} else {
				return goUint64Type
			}
		default:
			panic("bad integer size")
		}
	case T_FLOAT:
		switch t.Size() {
		case 4:
			return goFloat32Type
		case 8:
			return goFloat64Type
		default:
			// Should never happen
			panic("bad float size")
		}
	case T_TIME, T_BITFIELD, T_OPAQUE:
		// These cases, especially T_TIME, can't be supported in any
		// meaningful way. It's best for the library just to return
		// the raw bytes in the dataset.
		return goUint8Type
	case T_STRING:
		return goStringType
	case T_COMPOUND:
		return goStructType
	case T_REFERENCE:
		return goPtrType
	case T_VLEN:
		return goSliceType
	case T_ARRAY:
		return goArrayType
	default:
		// Should never happen
		panic("unknown TypeClass")
	}
}

// Close releases a datatype.
func (t *Datatype) Close() error {
	if t.id > 0 {
		err := h5err(C.H5Tclose(t.id))
		t.id = 0
		return err
	}
	return nil
}

// Committed determines whether a datatype is a named type or a transient type.
func (t *Datatype) Committed() bool {
	o := int(C.H5Tcommitted(t.id))
	if o > 0 {
		return true
	}
	return false
}

// Copy copies an existing datatype.
func (t *Datatype) Copy() (*Datatype, error) {
	return copyDatatype(t.id)
}

// copyDatatype should be called by any function wishing to return
// an existing Datatype from a Dataset or Attribute.
func copyDatatype(id C.hid_t) (*Datatype, error) {
	hid := C.H5Tcopy(id)
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	return NewDatatype(hid), nil
}

// Determines whether two datatype identifiers refer to the same datatype.
func (t *Datatype) Equal(o *Datatype) bool {
	v := int(C.H5Tequal(t.id, o.id))
	if v > 0 {
		return true
	}
	return false
}

// Lock locks a datatype.
func (t *Datatype) Lock() error {
	return h5err(C.H5Tlock(t.id))
}

// Size returns the size of the Datatype.
func (t *Datatype) Size() uint {
	return uint(C.H5Tget_size(t.id))
}

// SetSize sets the total size of a Datatype.
func (t *Datatype) SetSize(sz uint) error {
	err := C.H5Tset_size(t.id, C.size_t(sz))
	return h5err(err)
}

type ArrayType struct {
	Datatype
}

// NewArrayType creates a new ArrayType.
// base_type specifies the element type of the array.
// dims specify the dimensions of the array.
func NewArrayType(base_type *Datatype, dims []int) (*ArrayType, error) {
	ndims := C.uint(len(dims))
	c_dims := (*C.hsize_t)(unsafe.Pointer(&dims[0]))

	hid := C.H5Tarray_create2(base_type.id, ndims, c_dims)
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	t := &ArrayType{Datatype{Location{Identifier{hid}}}}
	runtime.SetFinalizer(t, (*ArrayType).finalizer)
	return t, err
}

// NDims returns the rank of an ArrayType.
func (t *ArrayType) NDims() int {
	return int(C.H5Tget_array_ndims(t.id))
}

// ArrayDims returns the array dimensions.
func (t *ArrayType) ArrayDims() []int {
	rank := t.NDims()
	dims := make([]int, rank)
	// fixme: int/hsize_t size!
	c_dims := (*C.hsize_t)(unsafe.Pointer(&dims[0]))
	c_rank := int(C.H5Tget_array_dims2(t.id, c_dims))
	if c_rank == rank {
		return dims
	}
	return nil
}

type VarLenType struct {
	Datatype
}

// NewVarLenType creates a new VarLenType.
// base_type specifies the element type of the VarLenType.
func NewVarLenType(base_type *Datatype) (*VarLenType, error) {
	id := C.H5Tvlen_create(base_type.id)
	err := h5err(C.herr_t(int(id)))
	if err != nil {
		return nil, err
	}
	t := &VarLenType{Datatype{Location{Identifier{id}}}}
	runtime.SetFinalizer(t, (*VarLenType).finalizer)
	return t, err
}

// IsVariableStr determines whether the VarLenType is a string.
func (vl *VarLenType) IsVariableStr() bool {
	o := int(C.H5Tis_variable_str(vl.id))
	if o > 0 {
		return true
	}
	return false
}

type CompoundType struct {
	Datatype
}

// NMembers returns the number of elements in a compound or enumeration datatype.
func (t *CompoundType) NMembers() int {
	return int(C.H5Tget_nmembers(t.id))
}

// Class returns the TypeClass of the DataType
func (t *Datatype) Class() TypeClass {
	return TypeClass(C.H5Tget_class(t.id))
}

// MemberClass returns datatype class of compound datatype member.
func (t *CompoundType) MemberClass(mbr_idx int) TypeClass {
	return TypeClass(C.H5Tget_member_class(t.id, C.uint(mbr_idx)))
}

// MemberName returns the name of a compound or enumeration datatype member.
func (t *CompoundType) MemberName(mbr_idx int) string {
	c_name := C.H5Tget_member_name(t.id, C.uint(mbr_idx))
	return C.GoString(c_name)
}

// MemberIndex returns the index of a compound or enumeration datatype member.
func (t *CompoundType) MemberIndex(name string) int {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	return int(C.H5Tget_member_index(t.id, c_name))
}

// MemberOffset returns the offset of a field of a compound datatype.
func (t *CompoundType) MemberOffset(mbr_idx int) int {
	return int(C.H5Tget_member_offset(t.id, C.uint(mbr_idx)))
}

// MemberType returns the datatype of the specified member.
func (t *CompoundType) MemberType(mbr_idx int) (*Datatype, error) {
	hid := C.H5Tget_member_type(t.id, C.uint(mbr_idx))
	err := h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	return copyDatatype(hid)
}

// Insert adds a new member to a compound datatype.
func (t *CompoundType) Insert(name string, offset int, field *Datatype) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return h5err(C.H5Tinsert(t.id, cname, C.size_t(offset), field.id))
}

// Pack recursively removes padding from within a compound datatype.
// This is analogous to C struct packing and will give a space-efficient
// type on the disk. However, using this may require type conversions
// on more machines, so may be a worse option.
func (t *CompoundType) Pack() error {
	return h5err(C.H5Tpack(t.id))
}

type OpaqueDatatype struct {
	Datatype
}

// Tags an opaque datatype.
func (t *OpaqueDatatype) SetTag(tag string) error {
	ctag := C.CString(tag)
	defer C.free(unsafe.Pointer(ctag))
	return h5err(C.H5Tset_tag(t.id, ctag))
}

// Gets the tag associated with an opaque datatype.
func (t *OpaqueDatatype) Tag() string {
	cname := C.H5Tget_tag(t.id)
	if cname != nil {
		return C.GoString(cname)
	}
	return ""
}

// NewDatatypeFromValue creates  a datatype from a value in an interface.
func NewDatatypeFromValue(v interface{}) (*Datatype, error) {
	return NewDatatypeFromType(reflect.TypeOf(v))
}

// NewDatatypeFromType creates a new Datatype from a reflect.Type.
func NewDatatypeFromType(t reflect.Type) (*Datatype, error) {

	var dt *Datatype = nil
	var err error

	switch t.Kind() {

	case reflect.Int:
		/* Special case logic is needed here as T_NATIVE_INT size can disagree
		with Go's int size. It's best to be explicit about type sizes
		when working with go-hdf5, to avoid writing implementation-dependent
		code. */

		if t.Bits() == 64 {
			// Commonest case, assuming 64 bit platforms running Go >=1.1.
			dt, err = T_NATIVE_INT64.Copy()
		} else if t.Bits() == 32 {
			// Less likely, but there are probably people running Go 1.0 or
			// or 32 bit.
			dt, err = T_NATIVE_INT32.Copy()
		} else {
			/* You are from the future or possibly an alternate dimension.
			(•_• )
			( •_•)>⌐■-■
			(⌐■_■)
			*/
			panic("implementation int size is not 32 or 64 bits")
		}

	case reflect.Int8:
		dt, err = T_NATIVE_INT8.Copy()

	case reflect.Int16:
		dt, err = T_NATIVE_INT16.Copy()

	case reflect.Int32:
		dt, err = T_NATIVE_INT32.Copy()

	case reflect.Int64:
		dt, err = T_NATIVE_INT64.Copy()

	case reflect.Uint: // Similar to Int, see above.
		if t.Bits() == 64 {
			dt, err = T_NATIVE_UINT64.Copy()
		} else if t.Bits() == 32 {
			dt, err = T_NATIVE_UINT32.Copy()
		} else {
			panic("implementation uint size is not 32 or 64 bits")
		}

	case reflect.Uint8:
		dt, err = T_NATIVE_UINT8.Copy()

	case reflect.Uint16:
		dt, err = T_NATIVE_UINT16.Copy()

	case reflect.Uint32:
		dt, err = T_NATIVE_UINT32.Copy()

	case reflect.Uint64:
		dt, err = T_NATIVE_UINT64.Copy()

	case reflect.Float32:
		dt, err = T_NATIVE_FLOAT.Copy()

	case reflect.Float64:
		dt, err = T_NATIVE_DOUBLE.Copy()

	case reflect.String:
		dt, err = T_GO_STRING.Copy()

	case reflect.Array:
		elem_type, err := NewDatatypeFromType(t.Elem())
		if err != nil {
			return nil, err
		}

		dims := getArrayDims(t)

		adt, err := NewArrayType(elem_type, dims)
		if err != nil {
			return nil, err
		}

		dt, err = adt.Copy()
		if err != nil {
			return nil, err
		}

	case reflect.Struct:
		sz := int(t.Size())
		hdf_dt, err := CreateDatatype(T_COMPOUND, sz)
		if err != nil {
			return nil, err
		}
		cdt := &CompoundType{*hdf_dt}
		n := t.NumField()
		for i := 0; i < n; i++ {
			f := t.Field(i)
			var field_dt *Datatype = nil
			field_dt, err = NewDatatypeFromType(f.Type)
			if err != nil {
				return nil, err
			}
			offset := int(f.Offset + 0)
			if field_dt == nil {
				return nil, fmt.Errorf("pb with field [%d-%s]", i, f.Name)
			}
			field_name := string(f.Tag)
			if len(field_name) == 0 {
				field_name = f.Name
			}
			err = cdt.Insert(field_name, offset, field_dt)
			if err != nil {
				return nil, fmt.Errorf("pb with field [%d-%s]: %s", i, f.Name, err)
			}
		}
		cdt.Lock()
		dt, err = cdt.Copy()
		if err != nil {
			return nil, err
		}

	default:
		// Should never happen.
		panic(fmt.Sprintf("unhandled kind (%v)", t.Kind()))
	}

	return dt, err
}

func getArrayDims(dt reflect.Type) []int {
	result := []int{}
	if dt.Kind() == reflect.Array {
		result = append(result, dt.Len())
		for _, dim := range getArrayDims(dt.Elem()) {
			result = append(result, dim)
		}
	}
	return result
}
