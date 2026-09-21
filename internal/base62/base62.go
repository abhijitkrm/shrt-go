package base62

const ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Encode renders n in base62 using ALPHABET ("0" for 0).
func Encode(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [11]byte // max uint64 is 11 base62 digits
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = ALPHABET[n%62]
		n /= 62
	}
	return string(buf[i:])
}
