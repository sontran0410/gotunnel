package session

// ring is a growable FIFO byte buffer.
type ring struct {
	buf  []byte
	head int // index of the first byte
	n    int // number of buffered bytes
}

// Len returns the number of buffered bytes.
func (r *ring) Len() int { return r.n }

// Write appends p, growing the buffer as needed.
func (r *ring) Write(p []byte) {
	if len(p) == 0 {
		return
	}
	r.grow(len(p))
	tail := (r.head + r.n) % len(r.buf)
	k := copy(r.buf[tail:], p)
	copy(r.buf, p[k:])
	r.n += len(p)
}

// CopyAt copies buffered bytes starting at logical offset off into p and
// returns how many were copied.
func (r *ring) CopyAt(p []byte, off int) int {
	if off >= r.n {
		return 0
	}
	n := min(len(p), r.n-off)
	start := (r.head + off) % len(r.buf)
	k := copy(p[:n], r.buf[start:])
	copy(p[k:n], r.buf)
	return n
}

// Discard drops the first k bytes.
func (r *ring) Discard(k int) {
	k = min(k, r.n)
	r.n -= k
	if r.n == 0 {
		r.head = 0
		if len(r.buf) > 1<<20 {
			r.buf = nil // release memory after bursts
		}
		return
	}
	r.head = (r.head + k) % len(r.buf)
}

// Read moves up to len(p) bytes from the front of the buffer into p.
func (r *ring) Read(p []byte) int {
	n := r.CopyAt(p, 0)
	r.Discard(n)
	return n
}

func (r *ring) grow(need int) {
	if r.n+need <= len(r.buf) {
		return
	}
	size := max(2*len(r.buf), 4096)
	for size < r.n+need {
		size *= 2
	}
	nb := make([]byte, size)
	if r.n > 0 {
		r.CopyAt(nb, 0)
	}
	r.buf = nb
	r.head = 0
}
