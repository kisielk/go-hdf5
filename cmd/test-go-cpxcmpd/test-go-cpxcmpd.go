package main

import (
	"fmt"

	"github.com/kisielk/go-hdf5"
)

const (
	FNAME   string = "SDScompound.h5"
	DATASET string = "ArrayOfStructures"
	MEMBER1 string = "A_name"
	MEMBER2 string = "B_name"
	MEMBER3 string = "C_name"
	LENGTH  uint   = 10
	RANK    int    = 1
)

type s1_t struct {
	a int
	b float32
	c float64
	d [3]int
	e string
}

type s2_t struct {
	c float64
	a int
}

func main() {

	// initialize data
	// s1 := make([]s1_t, LENGTH)
	// for i:=0; i<LENGTH; i++ {
	// 	s1[i] = s1_t{a:i, b:float32(i*i), c:1./(float64(i)+1)}
	// }
	// fmt.Printf(":: data: %v\n", s1)
	s1 := [LENGTH]s1_t{}
	for i := 0; i < int(LENGTH); i++ {
		s1[i] = s1_t{
			a: i,
			b: float32(i * i),
			c: 1. / (float64(i) + 1),
			d: [...]int{i, i * 2, i * 3},
			e: fmt.Sprintf("--%d--", i),
		}
		//s1[i].d = []float64{float64(i), float64(2*i), 3.*i}}
	}
	fmt.Printf(":: data: %v\n", s1)

	// create data space
	dims := []uint{LENGTH}
	space, err := hdf5.CreateSimpleDataspace(dims, nil)
	if err != nil {
		panic(err)
	}

	// create the file
	f, err := hdf5.CreateFile(FNAME, hdf5.F_ACC_TRUNC)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Printf(":: file [%s] created (id=%d)\n", FNAME, f.Id())

	// create the memory data type
	dtype, err := hdf5.NewDatatypeFromValue(s1[0])
	if err != nil {
		panic("could not create a dtype")
	}

	// create the dataset
	dset, err := f.CreateDataset(DATASET, dtype, space)
	if err != nil {
		panic(err)
	}
	fmt.Printf(":: dset (id=%d)\n", dset.Id())

	// write data to the dataset
	fmt.Printf(":: dset.Write...\n")
	err = dset.Write(&s1)
	if err != nil {
		panic(err)
	}
	fmt.Printf(":: dset.Write... [ok]\n")

	// release resources
	dset.Close()
	f.Close()

	// open the file and the dataset
	f, err = hdf5.OpenFile(FNAME, hdf5.F_ACC_RDONLY)
	if err != nil {
		panic(err)
	}
	dset, err = f.OpenDataset(DATASET)
	if err != nil {
		panic(err)
	}

	// read it back into a new slice
	s2 := make([]s1_t, LENGTH)
	dset.Read(s2)

	// display the fields
	fmt.Printf(":: data: %v\n", s2)

	// release resources
	dset.Close()
	f.Close()
}
