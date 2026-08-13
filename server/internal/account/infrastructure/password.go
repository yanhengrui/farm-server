package infrastructure

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	argonMemory      = 19 * 1024
	argonIterations  = 2
	argonParallelism = 1
	argonSaltLength  = 16
	argonKeyLength   = 32
)

func hashPassword(password string) ([]byte, error) {
	salt := make([]byte, argonSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generate password salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, argonIterations, argonMemory, argonParallelism, argonKeyLength)
	encoded := fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		argonMemory, argonIterations, argonParallelism,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash))
	return []byte(encoded), nil
}

func verifyPassword(encoded []byte, password string) bool {
	parts := strings.Split(string(encoded), "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	params := strings.Split(parts[3], ",")
	if len(params) != 3 {
		return false
	}
	memory, err1 := parseArgonParam(params[0], "m=")
	iterations, err2 := parseArgonParam(params[1], "t=")
	parallelism, err3 := parseArgonParam(params[2], "p=")
	if err1 != nil || err2 != nil || err3 != nil || memory == 0 || iterations == 0 || parallelism == 0 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(want) == 0 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, uint32(iterations), uint32(memory), uint8(parallelism), uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func parseArgonParam(value, prefix string) (uint64, error) {
	if !strings.HasPrefix(value, prefix) {
		return 0, errors.New("invalid argon2 parameter")
	}
	return strconv.ParseUint(strings.TrimPrefix(value, prefix), 10, 32)
}
