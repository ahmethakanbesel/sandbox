package main

import (
	"encoding/json"
	"testing"

	pb "protobuf-benchmark/person"

	"google.golang.org/protobuf/proto"
)

type Person struct {
	Name  string `json:"name"`
	Age   int32  `json:"age"`
	Email string `json:"email"`
}

var testPerson = Person{
	Name:  "John Doe",
	Age:   30,
	Email: "johndoe@example.com",
}

var testPersonProto = &pb.Person{
	Name:  "John Doe",
	Age:   30,
	Email: "johndoe@example.com",
}

func BenchmarkJSONMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testPerson)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	data, _ := json.Marshal(testPerson)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p Person
		err := json.Unmarshal(data, &p)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtobufMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(testPersonProto)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtobufUnmarshal(b *testing.B) {
	data, _ := proto.Marshal(testPersonProto)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p pb.Person
		err := proto.Unmarshal(data, &p)
		if err != nil {
			b.Fatal(err)
		}
	}
}
