package uuid

import (
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"reflect"
	"regexp"

	"github.com/pkg/errors"
)

const (
	// EncodedSize is the length of the text of the encoded UUID
	EncodedSize = 36
)

var (
	// Empty UUID is a UUID that is considered empty.
	Empty = UUID([EncodedSize]byte{})

	emptyUUID      = "00000000-0000-0000-0000-000000000000"
	emptyUUIDBytes = []byte{
		48, 48, 48, 48, 48, 48, 48, 48,
		45,
		48, 48, 48, 48,
		45,
		48, 48, 48, 48,
		45,
		48, 48, 48, 48,
		45,
		48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
	}
	layout = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
)

// UUID represents identifiers for content, resources and users
type UUID [EncodedSize]byte

// New generates a UUID from a random UUID source
func New(rnd *rand.Rand) (UUID, error) {
	return generate(rnd)
}

// MustNew create a UUID or panics on error
func MustNew(rnd *rand.Rand) UUID {
	id, err := New(rnd)
	if err != nil {
		panic(err)
	}
	return id
}

// Parse attempts to parse an id and return a UUID, or returns an error on
// failure.
func Parse(id string) (UUID, error) {
	return ParseBytes([]byte(id))
}

// ParseBytes attempts to parse an id and return a UUID, or returns an error on
// failure.
func ParseBytes(b []byte) (UUID, error) {
	if len(b) != EncodedSize {
		return Empty, errors.New("error invalid length")
	}

	if !layout.Match(b[:]) {
		return Empty, errors.New("error invalid layout")
	}

	res := [EncodedSize]byte{}
	for i := 0; i < EncodedSize; i++ {
		res[i] = b[i]
	}
	return UUID(res), nil
}

// MustParse parses the uuid or panics
func MustParse(id string) UUID {
	uid, err := Parse(id)
	if err != nil {
		panic(err)
	}
	return uid
}

// Bytes returns a series of bytes for the UUID
func (u UUID) Bytes() []byte {
	return u[:]
}

// Zero returns if the the UUID is zero or not
func (u UUID) Zero() bool {
	var amount int
	for _, v := range u {
		if v == 0 {
			amount++
		}
	}
	if amount == EncodedSize {
		return true
	}

	// Validate string
	for k, v := range u {
		if v != emptyUUIDBytes[k] {
			return false
		}
	}
	return true
}

func (u UUID) String() string {
	if u.Zero() {
		return emptyUUID
	}
	return string(u[:])
}

// Generate allows UUID to be used within quickcheck scenarios.
func (UUID) Generate(r *rand.Rand, size int) reflect.Value {
	id, err := generate(r)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(id)
}

// Equals checks that UUID equate to each other.
func (u UUID) Equals(id UUID) bool {
	for i := 0; i < EncodedSize; i++ {
		if u[i] != id[i] {
			return false
		}
	}
	return true
}

// MarshalJSON converts a UUID into a serialisable json format
func (u UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// UnmarshalJSON unserialises the json format and converts it into a UUID
func (u *UUID) UnmarshalJSON(b []byte) error {
	var res string
	if err := json.Unmarshal(b, &res); err != nil {
		return err
	}

	id, err := Parse(res)
	if err != nil {
		return err
	}

	for i := 0; i < EncodedSize; i++ {
		u[i] = id[i]
	}

	return nil
}

func generate(rnd *rand.Rand) (uuid [EncodedSize]byte, err error) {
	var (
		pos int
		r   = make([]byte, 16)
	)
	if pos, err = rnd.Read(r); err != nil {
		return
	} else if pos != 16 {
		err = errors.Errorf("generation failure (length)")
		return
	}

	r[6] = (r[6] & 0x0f) | 0x40 // Version 4
	r[8] = (r[8] & 0x3f) | 0x80 // Variant is 10

	hex.Encode(uuid[:], r[:4])
	uuid[8] = '-'
	hex.Encode(uuid[9:13], r[4:6])
	uuid[13] = '-'
	hex.Encode(uuid[14:18], r[6:8])
	uuid[18] = '-'
	hex.Encode(uuid[19:23], r[8:10])
	uuid[23] = '-'
	hex.Encode(uuid[24:], r[10:])

	return
}
