import secrets
import sys

if len(sys.argv) != 2:
    print(f'USAGE: {sys.argv[0]} OUTPUT_FILE', file=sys.stderr)
    sys.exit(1)

# What I found online (https://crypto.stackexchange.com/questions/35476/how-long-should-a-hmac-cryptographic-key-be)
# is that the key should be the same length as the hash, so since jsonwebtoken uses HS256 by default (as of v9.3.1),
# we generate a key of 256 bits
with open(sys.argv[1], 'wb') as f:
    f.write(secrets.token_bytes(256//8))
