package utils

import (
	"fmt"
	"reflect"
	"strings"
)

// SetNestedField modifies a nested struct field dynamically via reflection.
// obj: Pointer to the target struct.
// fieldPath: Dot-separated path (e.g., "Address.City", "Address.[json]city", "Address.[json]cit").
// newValue: Value to set.
func SetNestedField(obj interface{}, fieldPath string, newValue interface{}) error {
	val := reflect.ValueOf(obj)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("input must be a pointer to struct")
	}

	fields := strings.Split(fieldPath, ".")
	current := val.Elem() // Dereference the pointer

	for i := 0; i < len(fields); i++ {
		fieldSpec := fields[i]
		var fieldName string
		var field reflect.Value

		// Check if next field should use tag lookup
		if strings.HasPrefix(fieldSpec, "[") && strings.Contains(fieldSpec, "]") {
			// Extract tag name and value
			parts := strings.SplitN(fieldSpec[1:], "]", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid tag specification %q, expected format [tag]value", fieldSpec)
			}
			tagName := parts[0]
			tagValue := parts[1]

			// Find field by tag
			var found bool
			fieldName, found = findFieldByTag(current, tagName, tagValue)
			if !found {
				return fmt.Errorf("field with tag %s=%q not found in struct", tagName, tagValue)
			}
			field = current.FieldByName(fieldName)
		} else {
			// Regular field name
			fieldName = fieldSpec
			field = current.FieldByName(fieldName)
		}

		if !field.IsValid() {
			return fmt.Errorf("field %q not found", fieldName)
		}

		// If this is the last field, set its value
		if i == len(fields)-1 {
			if !field.CanSet() {
				return fmt.Errorf("field %q cannot be set (unexported?)", fieldName)
			}
			if newValue == nil {
				field.SetZero()
			} else {
				newVal := reflect.ValueOf(newValue)
				if field.Type() != newVal.Type() {
					return fmt.Errorf("type mismatch: %v != %v", field.Type(), newVal.Type())
				}
				field.Set(newVal)
			}

			return nil
		}

		// Move to next level
		switch field.Kind() {
		case reflect.Ptr:
			if field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			current = field.Elem()
		case reflect.Struct:
			current = field
		default:
			return fmt.Errorf("field %q is not a struct or pointer", fieldName)
		}
	}
	return nil
}

// findFieldByTag finds a struct field by its tag and returns the field name
func findFieldByTag(v reflect.Value, tagName, tagValue string) (string, bool) {
	tagValue = strings.ToLower(tagValue)
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get(tagName)
		if tag == "" {
			continue
		}

		// Handle cases where tag might have additional options
		tagParts := strings.Split(tag, ";")
		if strings.Contains(tagParts[0], tagValue) {
			return field.Name, true
		}
	}
	return "", false
}
