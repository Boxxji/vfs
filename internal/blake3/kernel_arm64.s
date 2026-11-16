// BLAKE3 compression, four chunks at a time, for arm64 NEON.
//
// Derived from c/blake3_neon.c of the BLAKE3 reference implementation,
// Copyright the BLAKE3 team, licensed under Apache-2.0 or CC0-1.0
// (https://github.com/BLAKE3-team/BLAKE3). The structure is the same: the four
// inputs are transposed so that vector register i holds message word i of all
// four, and one instruction stream then runs the compression of all four. The
// register allocation, the message loads and the loop are written for the Go
// assembler.

#include "textflag.h"

// Layout of lanes (kernel_arm64.go).
#define IN 0
#define CTR 32
#define KEY 64
#define OUT 96
#define BLOCKS 224
#define FLAGS 228
#define START 232
#define END 236
#define AHEAD 240
#define MSG 256

// Registers.
//   V0-V15   the sixteen state words, four lanes each
//   V16-V19  message words just loaded
//   V20-V23  the xor feeding a rotation
//   V24-V30  the chaining value, h0 to h6; h7 waits in memory, in the slot of
//            the last output, so that V31 can hold the table of rotate-by-8
//   V31      the byte permutation that rotates every word right by 8
//   R0       the lanes block
//   R1-R4    the four input pointers
//   R5       flags of the last block
//   R6       blocks left
//   R7       flags of every block
//   R8       flags of the first block
//   R9       flags of this block
//   R15      how far ahead of the inputs to prefetch

DATA ivs<>+0(SB)/4, $0x6A09E667
DATA ivs<>+4(SB)/4, $0x6A09E667
DATA ivs<>+8(SB)/4, $0x6A09E667
DATA ivs<>+12(SB)/4, $0x6A09E667
DATA ivs<>+16(SB)/4, $0xBB67AE85
DATA ivs<>+20(SB)/4, $0xBB67AE85
DATA ivs<>+24(SB)/4, $0xBB67AE85
DATA ivs<>+28(SB)/4, $0xBB67AE85
DATA ivs<>+32(SB)/4, $0x3C6EF372
DATA ivs<>+36(SB)/4, $0x3C6EF372
DATA ivs<>+40(SB)/4, $0x3C6EF372
DATA ivs<>+44(SB)/4, $0x3C6EF372
DATA ivs<>+48(SB)/4, $0xA54FF53A
DATA ivs<>+52(SB)/4, $0xA54FF53A
DATA ivs<>+56(SB)/4, $0xA54FF53A
DATA ivs<>+60(SB)/4, $0xA54FF53A
GLOBL ivs<>(SB), RODATA|NOPTR, $64

// Byte i of each word takes byte i+1 of the word: a rotate right by 8.
DATA rot8<>+0(SB)/8, $0x0407060500030201
DATA rot8<>+8(SB)/8, $0x0c0f0e0d080b0a09
GLOBL rot8<>(SB), RODATA|NOPTR, $16

// TRANSPOSE turns the 4x4 matrix of words in the rows a, b, c, d into its
// transpose, in place. t0-t3 are scratch.
#define TRANSPOSE(a, b, c, d, t0, t1, t2, t3) \
	VTRN1 b.S4, a.S4, t0.S4; \
	VTRN2 b.S4, a.S4, t1.S4; \
	VTRN1 d.S4, c.S4, t2.S4; \
	VTRN2 d.S4, c.S4, t3.S4; \
	VZIP1 t2.D2, t0.D2, a.D2; \
	VZIP1 t3.D2, t1.D2, b.D2; \
	VZIP2 t2.D2, t0.D2, c.D2; \
	VZIP2 t3.D2, t1.D2, d.D2

// LOADMSG loads words 4q to 4q+3 of this block from each of the four inputs and
// stores them transposed, so that MSG+16*(4q+j) holds word 4q+j of all four.
#define LOADMSG(q) \
	FMOVQ (16*q)(R1), F16; \
	FMOVQ (16*q)(R2), F17; \
	FMOVQ (16*q)(R3), F18; \
	FMOVQ (16*q)(R4), F19; \
	TRANSPOSE(V16, V17, V18, V19, V20, V21, V22, V23); \
	FMOVQ F16, (MSG+64*q)(R0); \
	FMOVQ F17, (MSG+64*q+16)(R0); \
	FMOVQ F18, (MSG+64*q+32)(R0); \
	FMOVQ F19, (MSG+64*q+48)(R0)

// ADD4 adds y0-y3 to x0-x3.
#define ADD4(y0, y1, y2, y3, x0, x1, x2, x3) \
	VADD y0.S4, x0.S4, x0.S4; \
	VADD y1.S4, x1.S4, x1.S4; \
	VADD y2.S4, x2.S4, x2.S4; \
	VADD y3.S4, x3.S4, x3.S4

// ADDMSG adds message words w0-w3 to x0-x3.
#define ADDMSG(w0, w1, w2, w3, x0, x1, x2, x3) \
	FMOVQ (MSG+16*w0)(R0), F16; \
	FMOVQ (MSG+16*w1)(R0), F17; \
	FMOVQ (MSG+16*w2)(R0), F18; \
	FMOVQ (MSG+16*w3)(R0), F19; \
	VADD V16.S4, x0.S4, x0.S4; \
	VADD V17.S4, x1.S4, x1.S4; \
	VADD V18.S4, x2.S4, x2.S4; \
	VADD V19.S4, x3.S4, x3.S4

// XOR4 puts x0^y0 to x3^y3 in V20-V23.
#define XOR4(x0, x1, x2, x3, y0, y1, y2, y3) \
	VEOR x0.B16, y0.B16, V20.B16; \
	VEOR x1.B16, y1.B16, V21.B16; \
	VEOR x2.B16, y2.B16, V22.B16; \
	VEOR x3.B16, y3.B16, V23.B16

// ROR16 sets x0-x3 to V20-V23 rotated right by 16.
#define ROR16(x0, x1, x2, x3) \
	VREV32 V20.H8, x0.H8; \
	VREV32 V21.H8, x1.H8; \
	VREV32 V22.H8, x2.H8; \
	VREV32 V23.H8, x3.H8

// ROR sets x0-x3 to V20-V23 rotated right by r, where l is 32-r.
#define ROR8(x0, x1, x2, x3) \
	VTBL V31.B16, [V20.B16], x0.B16; \
	VTBL V31.B16, [V21.B16], x1.B16; \
	VTBL V31.B16, [V22.B16], x2.B16; \
	VTBL V31.B16, [V23.B16], x3.B16

#define ROR(l, r, x0, x1, x2, x3) \
	VSHL $l, V20.S4, x0.S4; \
	VSRI $r, V20.S4, x0.S4; \
	VSHL $l, V21.S4, x1.S4; \
	VSRI $r, V21.S4, x1.S4; \
	VSHL $l, V22.S4, x2.S4; \
	VSRI $r, V22.S4, x2.S4; \
	VSHL $l, V23.S4, x3.S4; \
	VSRI $r, V23.S4, x3.S4

// MIX is the quarter round on four columns of the state at once: a, b, c, d are
// the registers of the four, x the message words added in the first half and y
// those added in the second.
#define MIX(a0, a1, a2, a3, b0, b1, b2, b3, c0, c1, c2, c3, d0, d1, d2, d3, x0, x1, x2, x3, y0, y1, y2, y3) \
	ADDMSG(x0, x1, x2, x3, a0, a1, a2, a3); \
	ADD4(b0, b1, b2, b3, a0, a1, a2, a3); \
	XOR4(a0, a1, a2, a3, d0, d1, d2, d3); \
	ROR16(d0, d1, d2, d3); \
	ADD4(d0, d1, d2, d3, c0, c1, c2, c3); \
	XOR4(b0, b1, b2, b3, c0, c1, c2, c3); \
	ROR(20, 12, b0, b1, b2, b3); \
	ADDMSG(y0, y1, y2, y3, a0, a1, a2, a3); \
	ADD4(b0, b1, b2, b3, a0, a1, a2, a3); \
	XOR4(a0, a1, a2, a3, d0, d1, d2, d3); \
	ROR8(d0, d1, d2, d3); \
	ADD4(d0, d1, d2, d3, c0, c1, c2, c3); \
	XOR4(b0, b1, b2, b3, c0, c1, c2, c3); \
	ROR(25, 7, b0, b1, b2, b3)

// ROUND mixes the columns and then the diagonals, with the message words in the
// order s0-s15 that the round's schedule gives.
#define ROUND(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15) \
	MIX(V0, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, s0, s2, s4, s6, s1, s3, s5, s7); \
	MIX(V0, V1, V2, V3, V5, V6, V7, V4, V10, V11, V8, V9, V15, V12, V13, V14, s8, s10, s12, s14, s9, s11, s13, s15)

// func hash4(l *lane4)
TEXT ·hash4(SB), NOSPLIT, $0-8
	MOVD l+0(FP), R0
	MOVD (IN+0)(R0), R1
	MOVD (IN+8)(R0), R2
	MOVD (IN+16)(R0), R3
	MOVD (IN+24)(R0), R4

	// The chaining value starts as the key, in every lane.
	ADD  $KEY, R0, R10
	VLD1R.P 4(R10), [V24.S4]
	VLD1R.P 4(R10), [V25.S4]
	VLD1R.P 4(R10), [V26.S4]
	VLD1R.P 4(R10), [V27.S4]
	VLD1R.P 4(R10), [V28.S4]
	VLD1R.P 4(R10), [V29.S4]
	VLD1R.P 4(R10), [V30.S4]
	VLD1R   (R10), [V23.S4]
	FMOVQ   F23, (OUT+112)(R0)
	MOVD    $rot8<>(SB), R14
	VLD1    (R14), [V31.B16]

	MOVWU BLOCKS(R0), R6
	MOVWU FLAGS(R0), R7
	MOVWU START(R0), R8
	MOVWU END(R0), R5
	MOVWU AHEAD(R0), R15
	ORR   R8, R7, R9
	MOVD  $ivs<>(SB), R11
	ADD   $CTR, R0, R12
	MOVW  $64, R13

block:
	CMP $1, R6
	BNE more
	ORR R5, R9, R9

more:
	// Fetch the lines of the next group of chunks, AHEAD bytes on, so that they
	// arrive before they are wanted: the hardware prefetcher sees four short
	// streams and does not hide the latency of memory.
	ADD  R15, R1, R10
	PRFM (R10), PLDL1KEEP
	ADD  R15, R2, R10
	PRFM (R10), PLDL1KEEP
	ADD  R15, R3, R10
	PRFM (R10), PLDL1KEEP
	ADD  R15, R4, R10
	PRFM (R10), PLDL1KEEP
	LOADMSG(0)
	LOADMSG(1)
	LOADMSG(2)
	LOADMSG(3)

	VMOV V24.B16, V0.B16
	VMOV V25.B16, V1.B16
	VMOV V26.B16, V2.B16
	VMOV V27.B16, V3.B16
	VMOV V28.B16, V4.B16
	VMOV V29.B16, V5.B16
	VMOV V30.B16, V6.B16
	FMOVQ (OUT+112)(R0), F7
	VLD1  (R11), [V8.S4, V9.S4, V10.S4, V11.S4]
	VLD1  (R12), [V12.S4, V13.S4]
	VDUP  R13, V14.S4
	VDUP  R9, V15.S4

	ROUND(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	ROUND(2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8)
	ROUND(3, 4, 10, 12, 13, 2, 7, 14, 6, 5, 9, 0, 11, 15, 8, 1)
	ROUND(10, 7, 12, 9, 14, 3, 13, 15, 4, 0, 11, 2, 5, 8, 1, 6)
	ROUND(12, 13, 9, 11, 15, 10, 14, 8, 7, 2, 5, 3, 0, 1, 6, 4)
	ROUND(9, 14, 11, 5, 8, 12, 15, 1, 13, 3, 0, 10, 2, 6, 4, 7)
	ROUND(11, 15, 5, 0, 1, 9, 8, 6, 14, 10, 2, 12, 3, 4, 7, 13)

	VEOR V8.B16, V0.B16, V24.B16
	VEOR V9.B16, V1.B16, V25.B16
	VEOR V10.B16, V2.B16, V26.B16
	VEOR V11.B16, V3.B16, V27.B16
	VEOR V12.B16, V4.B16, V28.B16
	VEOR V13.B16, V5.B16, V29.B16
	VEOR V14.B16, V6.B16, V30.B16
	VEOR V15.B16, V7.B16, V23.B16
	FMOVQ F23, (OUT+112)(R0)

	ADD  $64, R1
	ADD  $64, R2
	ADD  $64, R3
	ADD  $64, R4
	MOVD R7, R9
	SUBS $1, R6, R6
	BNE  block

	// The chaining values are in the columns of V24-V31; the outputs want them
	// in rows.
	TRANSPOSE(V24, V25, V26, V27, V16, V17, V18, V19)
	FMOVQ (OUT+112)(R0), F31
	TRANSPOSE(V28, V29, V30, V31, V16, V17, V18, V19)
	FMOVQ F24, (OUT+0)(R0)
	FMOVQ F28, (OUT+16)(R0)
	FMOVQ F25, (OUT+32)(R0)
	FMOVQ F29, (OUT+48)(R0)
	FMOVQ F26, (OUT+64)(R0)
	FMOVQ F30, (OUT+80)(R0)
	FMOVQ F27, (OUT+96)(R0)
	FMOVQ F31, (OUT+112)(R0)
	RET
