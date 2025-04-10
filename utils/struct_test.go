package utils_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/utils"
)

func TestSetNestedField(t *testing.T) {
	tests := []struct {
		name        string
		inputStruct interface{}
		fieldPath   string
		newValue    interface{}
		wantErr     bool
		wantField   string
		wantValue   interface{}
	}{
		{
			name: "Modify top-level field",
			inputStruct: &Person{
				Name: "Alice",
				Age:  30,
			},
			fieldPath: "Name",
			newValue:  "Bob",
			wantErr:   false,
			wantField: "Name",
			wantValue: "Bob",
		},
		{
			name: "Modify nested struct field",
			inputStruct: &Person{
				Address: Address{City: "Beijing"},
			},
			fieldPath: "Address.City",
			newValue:  "Shanghai",
			wantErr:   false,
			wantField: "Address.City",
			wantValue: "Shanghai",
		},
		{
			name: "Initialize nil pointer and modify",
			inputStruct: &Person{
				AddressPtr: nil,
			},
			fieldPath: "AddressPtr.City",
			newValue:  "New York",
			wantErr:   false,
			wantField: "AddressPtr.City",
			wantValue: "New York",
		},
		{
			name: "Modify field using json tag",
			inputStruct: &Person{
				Address: Address{City: "Beijing", Zip: 100000},
			},
			fieldPath: "Address.[json]city",
			newValue:  "Shanghai",
			wantErr:   false,
			wantField: "Address.City",
			wantValue: "Shanghai",
		},
		{
			name: "Modify field using json tag",
			inputStruct: &Person{
				Address: Address{City: "Beijing", Zip: 100000},
			},
			fieldPath: "Address.[json]City",
			newValue:  "Shanghai",
			wantErr:   false,
			wantField: "Address.City",
			wantValue: "Shanghai",
		},
		{
			name: "Modify field using json tag",
			inputStruct: &Person{
				Address: Address{City: "Beijing", Zip: 100000},
			},
			fieldPath: "Address.[json]cit",
			newValue:  "Shanghai",
			wantErr:   false,
			wantField: "Address.City",
			wantValue: "Shanghai",
		},
		{
			name:        "Non-pointer input",
			inputStruct: Person{},
			fieldPath:   "Name",
			newValue:    "Bob",
			wantErr:     true,
		},
		{
			name: "Non-existent field",
			inputStruct: &Person{
				Name: "Alice",
			},
			fieldPath: "InvalidField",
			newValue:  "Value",
			wantErr:   true,
		},
		{
			name: "Type mismatch",
			inputStruct: &Person{
				Age: 30,
			},
			fieldPath: "Age",
			newValue:  "Thirty", // string → int mismatch
			wantErr:   true,
		},
		{
			name: "Unexported field",
			inputStruct: &struct {
				name string // unexported
			}{},
			fieldPath: "name",
			newValue:  "Alice",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := utils.SetNestedField(tt.inputStruct, tt.fieldPath, tt.newValue)

			if (err != nil) != tt.wantErr {
				t.Errorf("%s: unexpected error status, got=%v, wantErr=%v", tt.name, err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				val := reflect.ValueOf(tt.inputStruct).Elem()
				fieldNames := strings.Split(tt.wantField, ".")
				for _, name := range fieldNames {
					if val.Kind() == reflect.Ptr {
						val = val.Elem()
					}
					val = val.FieldByName(name)
				}

				got := val.Interface()
				if !reflect.DeepEqual(got, tt.wantValue) {
					t.Errorf("%s: field value mismatch, got=%v, want=%v", tt.name, got, tt.wantValue)
				}
			}
		})
	}
}

// Person represents a test person structure
type Person struct {
	Name       string
	Age        int
	Address    Address  `json:"address"`
	AddressPtr *Address `json:"address_ptr"`
}

// Address represents a physical address with various tags for testing
type Address struct {
	City string `json:"city" custom:"city_name"`
	Zip  int    `json:"zip" custom:"zip_code"`
}
