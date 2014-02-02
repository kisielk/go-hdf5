package hdf5

import (
	"os"
	"reflect"
	"testing"
)

func withTestFile(name string, t func(f *File)) {
	f, err := CreateFile(name, F_ACC_TRUNC)
	if err != nil {
		panic("Couldn't create file!")
	}
	defer os.Remove(name)
	defer f.Close()
	t(f)
}

type DatasetTest struct {
	name    string
	dtype   *Datatype
	toWrite interface{}
}

func TestDatasetReadWrite(t *testing.T) {
	// Set up the dataspaces we'll be working with.
	ds0D, err := CreateDataspace(S_SCALAR)
	if err != nil {
		t.Fatal(err)
	}
	ds1D, err := CreateSimpleDataspace([]uint{4}, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataSpaces := []*Dataspace{ds0D, ds1D}

	testValues := [][]DatasetTest{
		{
			DatasetTest{"int80d", T_NATIVE_INT8, int8(-42)},
			DatasetTest{"int160d", T_NATIVE_INT16, int16(-42)},
			DatasetTest{"int320d", T_NATIVE_INT32, int32(-4200000)},
			DatasetTest{"int640d", T_NATIVE_INT64, int64(-4200000000)},

			DatasetTest{"uint80d", T_NATIVE_UINT8, uint8(42)},
			DatasetTest{"uint160d", T_NATIVE_UINT16, uint16(42)},
			DatasetTest{"uint320d", T_NATIVE_UINT32, uint32(42)},
			DatasetTest{"uint640d", T_NATIVE_UINT64, uint64(0xDEADBEEF)},

			DatasetTest{"float320d", T_NATIVE_FLOAT, float32(42.42)},
			DatasetTest{"float640d", T_NATIVE_DOUBLE, float64(42.42)},
			DatasetTest{"string0d", T_GO_STRING, "A man a plan a canal panama"},
		},
		{
			DatasetTest{"int81d", T_NATIVE_INT8, []int8{1, 2, -3, 0}},
			DatasetTest{"int161d", T_NATIVE_INT16, []int16{1, 2, -3, 0}},
			DatasetTest{"int321d", T_NATIVE_INT32, []int32{1, 2, -3, 0}},
			DatasetTest{"int641d", T_NATIVE_INT64, []int64{1, 2, -3, 0}},

			DatasetTest{"uint81d", T_NATIVE_UINT8, []uint8{1, 2, 3, 0}},
			DatasetTest{"uint161d", T_NATIVE_UINT16, []uint16{1, 2, 3, 0}},
			DatasetTest{"uint321d", T_NATIVE_UINT32, []uint32{1, 2, 3, 0}},
			DatasetTest{"uint641d", T_NATIVE_UINT64, []uint64{1, 2, 3, 0}},

			DatasetTest{"float321d", T_NATIVE_FLOAT, []float32{1.0, 2.0, 3.0, 4.0}},
			DatasetTest{"float641d", T_NATIVE_DOUBLE, []float64{1.0, 2.0, 3.0, 4.0}},
			DatasetTest{"string1d", T_GO_STRING, []string{"foo", "bar", "hullaballoo", "blah"}},
		},
	}

	withTestFile("dataset_test.h5", func(f *File) {
		for i := range testValues {
			ds := dataSpaces[i]
			for _, test := range testValues[i] {
				dset, err := f.CreateDataset(test.name, test.dtype, ds)
				if err != nil {
					t.Fatalf("CreateDataset failed: %s", err)
				}
				if name := dset.Name(); name != "/"+test.name {
					t.Fatalf("Bad name. got %v, want /%v", name, test.name)
				}
				if file := dset.File(); *file != *f {
					t.Fatal("Bad file. Got %v, want %v", *file, *f)
				}
				if err = dset.Write(test.toWrite); err != nil {
					t.Fatal(err)
				}
				f.Flush()

				result, err := dset.Read()
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(test.toWrite, result) {
					t.Errorf("%v != %v", test.toWrite, result)
				}
			}
		}
	})
}
