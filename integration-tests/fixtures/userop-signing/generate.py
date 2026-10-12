"""Independent manual ABI/Keccak fixtures. Requires pycryptodome==3.23.0.
Run from this directory: python3 generate.py > fixtures.json
No Engine, Alloy or generated ABI helper is used.
"""
import json
from Crypto.Hash import keccak

def digest(data):
    return keccak.new(digest_bits=256, data=data).digest()

def raw(value):
    return bytes.fromhex(value.removeprefix("0x"))

def word(value):
    return value.to_bytes(32, "big")

def address(value):
    return raw(value).rjust(32, b"\0")

def personal(value):
    return digest(b"\x19Ethereum Signed Message:\n32" + value)

admin = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
entrypoint = "0x1111111111111111111111111111111111111111"
profiles = [
    ("0.6", "0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00", "0xf22175c80c6e074C171811C59C6c0087e2a6a346"),
    ("0.7", "0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb", "0x94eC38a5d2EDA5A543Ab4c08D998338D4082beb2"),
]
cases = []
for version, factory, implementation in profiles:
    # Constructor's first CREATE links the verified factory to its Account.
    assert digest(bytes.fromhex("d694") + raw(factory) + b"\x01")[-20:] == raw(implementation)
    salt = digest(address(admin) + word(64) + word(0))  # abi.encode(admin, empty bytes)
    clone_init = raw("3d602d80600a3d3981f3363d3d373d3d3d363d73") + raw(implementation) + raw("5af43d82803e903d91602b57fd5bf3")
    sender = "0x" + digest(b"\xff" + raw(factory) + salt + digest(clone_init))[-20:].hex()
    op = {"sender": sender, "nonce": "0x7", "callData": "0x1234", "callGasLimit": "0x5208", "verificationGasLimit": "0x186a0", "preVerificationGas": "0xc350", "maxFeePerGas": "0x3b9aca00", "maxPriorityFeePerGas": "0x1", "signature": "0x"}
    head = address(sender) + word(7) + digest(b"") + digest(raw(op["callData"]))
    if version == "0.6":
        op.update(initCode="0x", paymasterAndData="0x")
        inner = head + word(21000) + word(100000) + word(50000) + word(1000000000) + word(1) + digest(b"")
    else:
        # Packed limits and fees are two uint128 values in one 32-byte word.
        inner = head + (100000).to_bytes(16, "big") + (21000).to_bytes(16, "big") + word(50000) + (1).to_bytes(16, "big") + (1000000000).to_bytes(16, "big") + digest(b"")
    def operation_hash(chain, ep):
        return digest(digest(inner) + address(ep) + word(chain))
    hashed = operation_hash(42161, entrypoint)
    cases.append({"version": version, "factory": factory, "implementation": implementation, "admin": admin, "accountSalt": "0x", "entrypoint": entrypoint, "chainId": 42161, "userop": op, "userOpHash": "0x" + hashed.hex(), "contractDigest": "0x" + personal(hashed).hex(), "otherChainDigest": "0x" + personal(operation_hash(1, entrypoint)).hex(), "otherEntrypointDigest": "0x" + personal(operation_hash(42161, "0x2222222222222222222222222222222222222222")).hex()})
print(json.dumps({"generator": "manual ABI + PyCryptodome 3.23.0 Keccak; AccountCore EIP-191 bytes32 policy", "cases": cases}, indent=2))
