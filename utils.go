package machinery

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

const (
	LockKeyPrefix = "machinery_lock_"
)

func GetLockName(name, spec string) string {
	return LockKeyPrefix + filepath.Base(os.Args[0]) + name + spec
}

var (
	ErrNoMatchType     = errors.New("no match type")
	ErrNoPointer       = errors.New("must be interface")
	ErrInvalidArgument = errors.New("invalid arguments")
)

func deepCopy(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Interface:
		value := src.Elem()
		if !value.IsValid() {
			return
		}
		newValue := reflect.New(value.Type()).Elem()
		deepCopy(newValue, value)
		dst.Set(newValue)
	case reflect.Ptr:
		value := src.Elem()
		if !value.IsValid() {
			return
		}
		dst.Set(reflect.New(value.Type()))
		deepCopy(dst.Elem(), value)
	case reflect.Map:
		dst.Set(reflect.MakeMap(src.Type()))
		keys := src.MapKeys()
		for _, key := range keys {
			value := src.MapIndex(key)
			newValue := reflect.New(value.Type()).Elem()
			deepCopy(newValue, value)
			dst.SetMapIndex(key, newValue)
		}
	case reflect.Slice:
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
		for i := 0; i < src.Len(); i++ {
			deepCopy(dst.Index(i), src.Index(i))
		}
	case reflect.Struct:
		typeSrc := src.Type()
		for i := 0; i < src.NumField(); i++ {
			value := src.Field(i)
			tag := typeSrc.Field(i).Tag
			if value.CanSet() && tag.Get("deepcopy") != "-" {
				deepCopy(dst.Field(i), value)
			}
		}
	default:
		dst.Set(src)
	}
}

func DeepCopy(dst, src any) error {
	typeDst := reflect.TypeOf(dst)
	typeSrc := reflect.TypeOf(src)
	if typeDst != typeSrc {
		return ErrNoMatchType
	}
	if typeSrc.Kind() != reflect.Ptr {
		return ErrNoPointer
	}

	valueDst := reflect.ValueOf(dst).Elem()
	valueSrc := reflect.ValueOf(src).Elem()
	if !valueDst.IsValid() || !valueSrc.IsValid() {
		return ErrInvalidArgument
	}

	deepCopy(valueDst, valueSrc)
	return nil
}

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := Fibonacci()
	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)

			slog.Warn("retrying broker connection", "retryInSeconds", retryIn)

			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}

// Fibonacci returns successive Fibonacci numbers starting from 1
func Fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

// FibonacciNext returns next number in Fibonacci sequence greater than start
func FibonacciNext(start int) int {
	fib := Fibonacci()
	num := fib()
	for num <= start {
		num = fib()
	}
	return num
}
