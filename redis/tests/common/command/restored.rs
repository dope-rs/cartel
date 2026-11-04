pub const SCRIPT: [(&[u8], &[u8]); 46] = [
    (
        b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n$2\r\nPX\r\n$4\r\n1250\r\n",
        b"+OK\r\n",
    ),
    (
        b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n$2\r\nNX\r\n",
        b"+OK\r\n",
    ),
    (
        b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n$2\r\nNX\r\n",
        b"$-1\r\n",
    ),
    (b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n60\r\n", b":1\r\n"),
    (
        b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nkey\r\n$4\r\n1.25\r\n",
        b"$3\r\n2.5\r\n",
    ),
    (
        b"*5\r\n$6\r\nZRANGE\r\n$5\r\nboard\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n",
        b"*2\r\n$1\r\na\r\n$3\r\n2.5\r\n",
    ),
    (
        b"*4\r\n$13\r\nZRANGEBYSCORE\r\n$5\r\nboard\r\n$4\r\n-inf\r\n$3\r\ninf\r\n",
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
    ),
    (b"*4\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n", b":2\r\n"),
    (b"*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n", b":4\r\n"),
    (b"*2\r\n$4\r\nLPOP\r\n$4\r\nlist\r\n", b"$1\r\nb\r\n"),
    (b"*2\r\n$4\r\nRPOP\r\n$4\r\nlist\r\n", b"$-1\r\n"),
    (
        b"*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
    ),
    (b"*2\r\n$4\r\nLLEN\r\n$4\r\nlist\r\n", b":2\r\n"),
    (b"*2\r\n$8\r\nBITCOUNT\r\n$4\r\nbits\r\n", b":7\r\n"),
    (
        b"*4\r\n$8\r\nBITCOUNT\r\n$4\r\nbits\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b":7\r\n",
    ),
    (b"*4\r\n$6\r\nSETBIT\r\n$4\r\nbits\r\n$1\r\n9\r\n$1\r\n1\r\n", b":0\r\n"),
    (b"*3\r\n$6\r\nGETBIT\r\n$4\r\nbits\r\n$1\r\n9\r\n", b":1\r\n"),
    (b"*3\r\n$6\r\nBITPOS\r\n$4\r\nbits\r\n$1\r\n0\r\n", b":-1\r\n"),
    (
        b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$3\r\nout\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    ),
    (
        b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$3\r\nout\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    ),
    (
        b"*5\r\n$5\r\nBITOP\r\n$3\r\nXOR\r\n$3\r\nout\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    ),
    (b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$3\r\nout\r\n$1\r\na\r\n", b":1\r\n"),
    (b"*4\r\n$5\r\nPFADD\r\n$3\r\nhll\r\n$1\r\na\r\n$1\r\nb\r\n", b":1\r\n"),
    (b"*2\r\n$5\r\nPFADD\r\n$3\r\nhll\r\n", b":0\r\n"),
    (b"*3\r\n$7\r\nPFCOUNT\r\n$1\r\na\r\n$1\r\nb\r\n", b":2\r\n"),
    (b"*4\r\n$7\r\nPFMERGE\r\n$3\r\nout\r\n$1\r\na\r\n$1\r\nb\r\n", b"+OK\r\n"),
    (b"*2\r\n$7\r\nPFMERGE\r\n$3\r\nout\r\n", b"+OK\r\n"),
    (
        b"*5\r\n$6\r\nGEOADD\r\n$3\r\nmap\r\n$4\r\n1.25\r\n$4\r\n-2.5\r\n$1\r\na\r\n",
        b":1\r\n",
    ),
    (b"*4\r\n$7\r\nGEODIST\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n", b"$-1\r\n"),
    (
        b"*5\r\n$7\r\nGEODIST\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nm\r\n",
        b"$4\r\n4.25\r\n",
    ),
    (
        b"*5\r\n$7\r\nGEODIST\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nkm\r\n",
        b"$4\r\n4.25\r\n",
    ),
    (
        b"*5\r\n$7\r\nGEODIST\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nmi\r\n",
        b"$4\r\n4.25\r\n",
    ),
    (
        b"*5\r\n$7\r\nGEODIST\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nft\r\n",
        b"$4\r\n4.25\r\n",
    ),
    (
        b"*4\r\n$6\r\nGEOPOS\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n",
        b"*2\r\n*2\r\n$4\r\n1.25\r\n$4\r\n-2.5\r\n*-1\r\n",
    ),
    (
        b"*8\r\n$9\r\nGEOSEARCH\r\n$3\r\nmap\r\n$10\r\nFROMLONLAT\r\n$4\r\n1.25\r\n$4\r\n-2.5\r\n$8\r\nBYRADIUS\r\n$3\r\n3.5\r\n$2\r\nkm\r\n",
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
    ),
    (
        b"*3\r\n$6\r\nGEOPOS\r\n$3\r\nmap\r\n$1\r\na\r\n",
        b"*1\r\n*1\r\n$4\r\n1.25\r\n",
    ),
    (
        b"*3\r\n$6\r\nGEOPOS\r\n$3\r\nmap\r\n$1\r\na\r\n",
        b"*1\r\n+bad\r\n",
    ),
    (
        b"*4\r\n$6\r\nGEOPOS\r\n$3\r\nmap\r\n$1\r\na\r\n$1\r\nb\r\n",
        b"*2\r\n*2\r\n$4\r\n1.25\r\n$4\r\n-2.5\r\n*2\r\n$3\r\n3.5\r\n$3\r\nx.y\r\n",
    ),
    (b"*2\r\n$6\r\nGEOPOS\r\n$3\r\nmap\r\n", b"*0\r\n"),
    (b"*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n", b"+string\r\n"),
    (b"*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n", b"+OK\r\n"),
    (
        b"*6\r\n$4\r\nSCAN\r\n$2\r\n17\r\n$5\r\nMATCH\r\n$2\r\na*\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
        b"*2\r\n$1\r\n0\r\n*0\r\n",
    ),
    (
        b"*5\r\n$5\r\nZSCAN\r\n$5\r\nboard\r\n$2\r\n17\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
        b"*2\r\n$2\r\n23\r\n*4\r\n$1\r\na\r\n$3\r\n2.5\r\n$1\r\nb\r\n$2\r\n-1\r\n",
    ),
    (b"*1\r\n$4\r\nPING\r\n", b"*2\r\n*0\r\n*2\r\n$-1\r\n:7\r\n"),
    (
        b"*1\r\n$4\r\nPING\r\n",
        b"*2\r\n*2\r\n:1\r\n-ERR first\r\n-ERR later\r\n",
    ),
    (b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n"),
];
