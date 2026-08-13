package infrastructure

import "testing"

func TestPasswordHashRoundTrip(t *testing.T) {
	hash, err := hashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !verifyPassword(hash, "correct horse battery staple") {
		t.Fatal("correct password was rejected")
	}
	if verifyPassword(hash, "wrong password") {
		t.Fatal("wrong password was accepted")
	}
}

func TestPasswordHashUsesRandomSalt(t *testing.T) {
	first, err := hashPassword("same password")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hashPassword("same password")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatal("password hashes unexpectedly match")
	}
}
