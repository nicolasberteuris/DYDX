#######################################################################
# Exchange Adapter Definition - dYdX Class
#
# Notes:
# Sub-accounts are managed via APIs
#
#######################################################################
import asyncio
from typing import Tuple
import decimal
import json
import hmac
import hashlib
import base64
import json
import os
import time
import hashlib
import websockets
import urllib
#from urllib.parse import urlencode
#from library import Helper
#from adapters.protocols import Rest
from datetime import datetime
from datetime import timezone
#from library.log import LOG
from web3 import Web3
import os
from dotenv import load_dotenv
import math
import dateutil.parser as dp
from collections import namedtuple
import ctypes
import secrets
import os
from typing import Optional, Tuple
import json
import math
import hashlib
import json
import math
import os
import random
from typing import Optional, Tuple, Union
from ecdsa.rfc6979 import generate_k
from adapters.protocols import Rest
#from adapters.exchanges.dydx_constants import *
from typing import Tuple
import mpmath
import sympy
from sympy.core.numbers import igcdex
import requests



ECPoint = Tuple[int, int]
ECSignature = Tuple[int, int]

class Signable(object):
    """Base class for an object signable with a STARK key."""

    def __init__(self, network_id, message):
        self.network_id = network_id
        self._message = message
        self._hash = None

        # Sanity check.
        if not COLLATERAL_ASSET_ID_BY_NETWORK_ID[self.network_id]:
            raise ValueError(
                'Unknown network ID or unknown collateral asset for network: '
                '{}'.format(network_id),
            )

    @property
    def hash(self):
        """Get the hash of the object."""
        if self._hash is None:
            self._hash = self._calculate_hash()
        return self._hash

    def sign(self, private_key_hex):
        """Sign the hash of the object using the given private key."""
        r, s = sign(self.hash, int(private_key_hex, 16))
        return serialize_signature(r, s)

    def verify_signature(self, signature_hex, public_key_hex):
        """Return True if the signature is valid for the given public key."""
        r, s = deserialize_signature(signature_hex)
        return verify(self.hash, r, s, int(public_key_hex, 16))

    def _calculate_hash(self):
        raise NotImplementedError


class SignableOrder(Signable):

    def __init__(
        self,
        network_id,
        market,
        side,
        position_id,
        human_size,
        human_price,
        limit_fee,
        client_id,
        expiration_epoch_seconds,
    ):
        synthetic_asset = SYNTHETIC_ASSET_MAP[market]
        synthetic_asset_id = SYNTHETIC_ASSET_ID_MAP[synthetic_asset]
        collateral_asset_id = COLLATERAL_ASSET_ID_BY_NETWORK_ID[network_id]
        is_buying_synthetic = side == ORDER_SIDE_BUY
        quantums_amount_synthetic = to_quantums_exact(
            human_size,
            synthetic_asset,
        )

        # Note: By creating the decimals outside the context and then
        # multiplying within the context, we ensure rounding does not occur
        # until after the multiplication is computed with full precision.
        if is_buying_synthetic:
            human_cost = DECIMAL_CONTEXT_ROUND_UP.multiply(
                decimal.Decimal(human_size),
                decimal.Decimal(human_price)
            )
            quantums_amount_collateral = to_quantums_round_up(
                human_cost,
                COLLATERAL_ASSET,
            )
        else:
            human_cost = DECIMAL_CONTEXT_ROUND_DOWN.multiply(
                decimal.Decimal(human_size),
                decimal.Decimal(human_price)
            )
            quantums_amount_collateral = to_quantums_round_down(
                human_cost,
                COLLATERAL_ASSET,
            )

        # The limitFee is a fraction, e.g. 0.01 is a 1 % fee.
        # It is always paid in the collateral asset.
        # Constrain the limit fee to six decimals of precision.
        # The final fee amount must be rounded up.
        limit_fee_rounded = DECIMAL_CONTEXT_ROUND_DOWN.quantize(
            decimal.Decimal(limit_fee),
            decimal.Decimal('0.000001'),
        )
        quantums_amount_fee_decimal = DECIMAL_CONTEXT_ROUND_UP.multiply(
            limit_fee_rounded,
            quantums_amount_collateral,
        ).to_integral_value(context=DECIMAL_CONTEXT_ROUND_UP)

        # Orders may have a short time-to-live on the orderbook, but we need
        # to ensure their signatures are valid by the time they reach the
        # blockchain. Therefore, we enforce that the signed expiration includes
        # a buffer relative to the expiration timestamp sent to the dYdX API.
        expiration_epoch_hours = math.ceil(
            float(expiration_epoch_seconds) / ONE_HOUR_IN_SECONDS,
        ) + ORDER_SIGNATURE_EXPIRATION_BUFFER_HOURS

        message = StarkwareOrder(
            order_type='LIMIT_ORDER_WITH_FEES',
            asset_id_synthetic=synthetic_asset_id,
            asset_id_collateral=collateral_asset_id,
            asset_id_fee=collateral_asset_id,
            quantums_amount_synthetic=quantums_amount_synthetic,
            quantums_amount_collateral=quantums_amount_collateral,
            quantums_amount_fee=int(quantums_amount_fee_decimal),
            is_buying_synthetic=is_buying_synthetic,
            position_id=int(position_id),
            nonce=nonce_from_client_id(client_id),
            expiration_epoch_hours=expiration_epoch_hours,
        )
        super(SignableOrder, self).__init__(network_id, message)

    def to_starkware(self):
        return self._message

    def _calculate_hash(self):
        """Calculate the hash of the Starkware order."""

        # TODO: Check values are in bounds

        if self._message.is_buying_synthetic:
            asset_id_sell = self._message.asset_id_collateral
            asset_id_buy = self._message.asset_id_synthetic
            quantums_amount_sell = self._message.quantums_amount_collateral
            quantums_amount_buy = self._message.quantums_amount_synthetic
        else:
            asset_id_sell = self._message.asset_id_synthetic
            asset_id_buy = self._message.asset_id_collateral
            quantums_amount_sell = self._message.quantums_amount_synthetic
            quantums_amount_buy = self._message.quantums_amount_collateral

        part_1 = quantums_amount_sell
        part_1 <<= ORDER_FIELD_BIT_LENGTHS['quantums_amount']
        part_1 += quantums_amount_buy
        part_1 <<= ORDER_FIELD_BIT_LENGTHS['quantums_amount']
        part_1 += self._message.quantums_amount_fee
        part_1 <<= ORDER_FIELD_BIT_LENGTHS['nonce']
        part_1 += self._message.nonce

        part_2 = ORDER_PREFIX
        for _ in range(3):
            part_2 <<= ORDER_FIELD_BIT_LENGTHS['position_id']
            part_2 += self._message.position_id
        part_2 <<= ORDER_FIELD_BIT_LENGTHS['expiration_epoch_hours']
        part_2 += self._message.expiration_epoch_hours
        part_2 <<= ORDER_PADDING_BITS

        assets_hash = get_hash(
            get_hash(
                asset_id_sell,
                asset_id_buy,
            ),
            self._message.asset_id_fee,
        )
        return get_hash(
            get_hash(
                assets_hash,
                part_1,
            ),
            part_2,
        )







def get_cpp_lib(crypto_c_exports_path):
    global CPP_LIB_PATH
    CPP_LIB_PATH = ctypes.cdll.LoadLibrary(os.path.abspath(crypto_c_exports_path))
    # Configure argument and return types.
    CPP_LIB_PATH.Hash.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    CPP_LIB_PATH.Verify.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    CPP_LIB_PATH.Verify.restype = bool
    CPP_LIB_PATH.Sign.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]

def check_cpp_lib_path() -> bool:
  return CPP_LIB_PATH is not None



#################
# CPP WRAPPERS #
#################

def cpp_hash(left, right) -> int:
    res = ctypes.create_string_buffer(OUT_BUFFER_SIZE)
    if CPP_LIB_PATH.Hash(
            left.to_bytes(32, 'little', signed=False),
            right.to_bytes(32, 'little', signed=False),
            res) != 0:
        raise ValueError(res.raw.rstrip(b'\00'))
    return int.from_bytes(res.raw[:32], 'little', signed=False)


def cpp_sign(msg_hash, priv_key, seed: Optional[int] = 32) -> ECSignature:
    """
    Note that this uses the secrets module to generate cryptographically strong random numbers.
    Note that the same seed will give a different signature compared with the sign function in
    signature.py.
    """
    res = ctypes.create_string_buffer(OUT_BUFFER_SIZE)
    random_bytes = secrets.token_bytes(seed)
    if CPP_LIB_PATH.Sign(
            priv_key.to_bytes(32, 'little', signed=False),
            msg_hash.to_bytes(32, 'little', signed=False),
            random_bytes, res) != 0:
        raise ValueError(res.raw.rstrip(b'\00'))
    w = int.from_bytes(res.raw[32:64], 'little', signed=False)
    s = inv_mod_curve_size(w)
    return (int.from_bytes(res.raw[:32], 'little', signed=False), s)


def cpp_verify(msg_hash, r, s, stark_key) -> bool:
    w =inv_mod_curve_size(s)
    assert 1 <= stark_key < 2**N_ELEMENT_BITS_ECDSA, 'stark_key = %s' % stark_key
    assert 1 <= msg_hash < 2**N_ELEMENT_BITS_ECDSA, 'msg_hash = %s' % msg_hash
    assert 1 <= r < 2**N_ELEMENT_BITS_ECDSA, 'r = %s' % r
    assert 1 <= w < EC_ORDER, 'w = %s' % w
    return CPP_LIB_PATH.Verify(
        stark_key.to_bytes(32, 'little', signed=False),
        msg_hash.to_bytes(32, 'little', signed=False),
        r.to_bytes(32, 'little', signed=False),
        w.to_bytes(32, 'little', signed=False))

def sign(
    msg_hash: int,
    priv_key: int,
    seed: Optional[int] = None,
) -> ECSignature:
    # Note: cpp_sign() is not optimized and is currently slower than py_sign().
    #       So always use py_sign() for now.
    return py_sign(msg_hash=msg_hash, priv_key=priv_key, seed=seed)


def verify(
    msg_hash: int,
    r: int,
    s: int,
    public_key: Union[int, ECPoint],
) -> bool:
    if check_cpp_lib_path():
        return cpp_verify(msg_hash=msg_hash, r=r, s=s, stark_key=public_key)

    return py_verify(msg_hash=msg_hash, r=r, s=s, public_key=public_key)


def get_hash(*elements: int) -> int:
    if check_cpp_lib_path():
        return cpp_hash(*elements)

    return py_pedersen_hash(*elements)



def bytes_to_int(x):
    """Convert a bytestring to an int."""
    return int(x.hex(), 16)


def int_to_hex_32(x):
    """Normalize to a 32-byte hex string without 0x prefix."""
    padded_hex = hex(x)[2:].rjust(64, '0')
    if len(padded_hex) != 64:
        raise ValueError('Input does not fit in 32 bytes')
    return padded_hex


def serialize_signature(r, s):
    """Convert a signature from an r, s pair to a 32-byte hex string."""
    return int_to_hex_32(r) + int_to_hex_32(s)


def deserialize_signature(signature):
    """Convert a signature from a 32-byte hex string to an r, s pair."""
    if len(signature) != 128:
        raise ValueError(
            'Invalid serialized signature, expected hex string of length 128',
        )
    return int(signature[:64], 16), int(signature[64:], 16)


def to_quantums_exact(human_amount, asset):
    """Convert a human-readable amount to an integer amount of quantums.
    If the provided human_amount is not a multiple of the quantum size,
    an exception will be raised.
    """
    return _to_quantums_helper(human_amount, asset, DECIMAL_CTX_EXACT)


def to_quantums_round_down(human_amount, asset):
    """Convert a human-readable amount to an integer amount of quantums.
    If the provided human_amount is not a multiple of the quantum size,
    the result will be rounded down to the nearest integer.
    """
    return _to_quantums_helper(human_amount, asset, DECIMAL_CTX_ROUND_DOWN)


def to_quantums_round_up(human_amount, asset):
    """Convert a human-readable amount to an integer amount of quantums.
    If the provided human_amount is not a multiple of the quantum size,
    the result will be rounded up to the nearest integer.
    """
    return _to_quantums_helper(human_amount, asset, DECIMAL_CTX_ROUND_UP)


def _to_quantums_helper(human_amount, asset, ctx):
    try:
        amount_dec = ctx.create_decimal(human_amount)
        resolution_dec = ctx.create_decimal(ASSET_RESOLUTION[asset])
        quantums = (amount_dec * resolution_dec).to_integral_exact(context=ctx)
    except decimal.Inexact:
        raise ValueError(
            'Amount {} is not a multiple of the quantum size {}'.format(
                human_amount,
                1 / float(ASSET_RESOLUTION[asset]),
            ),
        )
    return int(quantums)


def nonce_from_client_id(client_id):
    """Generate a nonce deterministically from an arbitrary string."""
    message = hashlib.sha256()
    message.update(client_id.encode())  # Encode as UTF-8.
    return int(message.digest().hex(), 16) % NONCE_UPPER_BOUND_EXCLUSIVE


def get_transfer_erc20_fact(
    recipient,
    token_decimals,
    human_amount,
    token_address,
    salt,
):
    token_amount = float(human_amount) * (10 ** token_decimals)
    if not token_amount.is_integer():
        raise ValueError(
            'Amount {} has more precision than token decimals {}'.format(
                human_amount,
                token_decimals,
            )
        )
    hex_bytes = Web3.solidityKeccak(
        [
            'address',
            'uint256',
            'address',
            'uint256',
        ],
        [
            recipient,
            int(token_amount),
            token_address,
            salt,
        ],
    )
    return bytes(hex_bytes)

def strip_hex_prefix(input):
    if input.startswith('0x'):
        return input[2:]

    return input


def fact_to_condition(fact_registry_address, fact):
    """Generate the condition, signed as part of a conditional transfer."""
    if not isinstance(fact, bytes):
        raise ValueError('fact must be a byte-string')
    data = bytes.fromhex(strip_hex_prefix(fact_registry_address)) + fact
    return int(Web3.keccak(data).hex(), 16) & BIT_MASK_250


def message_to_hash(message_string):
    """Generate a hash deterministically from an arbitrary string."""
    message = hashlib.sha256()
    message.update(message_string.encode())  # Encode as UTF-8.
    return int(message.digest().hex(), 16) >> 5


def generate_private_key_hex_unsafe():
    """Generate a STARK key using the Python builtin random module."""
    return hex(get_random_private_key())


def private_key_from_bytes(data):
    """Generate a STARK key deterministically from binary data."""
    if not isinstance(data, bytes):
        raise ValueError('Input must be a byte-string')
    return hex(int(Web3.keccak(data).hex(), 16) >> 5)


def private_key_to_public_hex(private_key_hex):
    """Given private key as hex string, return the public key as hex string."""
    private_key_int = int(private_key_hex, 16)
    return hex(private_to_stark_key(private_key_int))


def private_key_to_public_key_pair_hex(private_key_hex):
    """Given private key as hex string, return the public x, y pair as hex."""
    private_key_int = int(private_key_hex, 16)
    x, y = private_key_to_ec_point_on_stark_curve(private_key_int)
    return [hex(x), hex(y)]





#########
# ECDSA #
#########



class InvalidPublicKeyError(Exception):
    def __init__(self):
        super().__init__('Given x coordinate does not represent any point on the elliptic curve.')


def get_y_coordinate(stark_key_x_coordinate: int) -> int:
    """
    Given the x coordinate of a stark_key, returns a possible y coordinate such that together the
    point (x,y) is on the curve.
    Note that the real y coordinate is either y or -y.
    If x is invalid stark_key it throws an error.
    """

    x = stark_key_x_coordinate
    y_squared = (x * x * x + ALPHA * x + BETA) % FIELD_PRIME
    if not is_quad_residue(y_squared, FIELD_PRIME):
        raise InvalidPublicKeyError()
    return sqrt_mod(y_squared, FIELD_PRIME)


def get_random_private_key() -> int:
    # NOTE: It is IMPORTANT to use a strong random function here.
    return random.randint(1, EC_ORDER - 1)


def private_key_to_ec_point_on_stark_curve(priv_key: int) -> ECPoint:
    assert 0 < priv_key < EC_ORDER
    return ec_mult(priv_key, EC_GEN, ALPHA, FIELD_PRIME)


def private_to_stark_key(priv_key: int) -> int:
    return private_key_to_ec_point_on_stark_curve(priv_key)[0]


def inv_mod_curve_size(x: int) -> int:
    return div_mod(1, x, EC_ORDER)


def generate_k_rfc6979(msg_hash: int, priv_key: int, seed: Optional[int] = None) -> int:
    # Pad the message hash, for consistency with the elliptic.js library.
    if 1 <= msg_hash.bit_length() % 8 <= 4 and msg_hash.bit_length() >= 248:
        # Only if we are one-nibble short:
        msg_hash *= 16

    if seed is None:
        extra_entropy = b''
    else:
        extra_entropy = seed.to_bytes(math.ceil(seed.bit_length() / 8), 'big')

    return generate_k(EC_ORDER, priv_key, hashlib.sha256,
                      msg_hash.to_bytes(math.ceil(msg_hash.bit_length() / 8), 'big'),
                      extra_entropy=extra_entropy)


 # Starkware crypto functions implemented in Python.
 #
 # Copied from:
 # https://github.com/starkware-libs/starkex-resources/blob/0f08e6c55ad88c93499f71f2af4a2e7ae0185cdf/crypto/starkware/crypto/signature/signature.py
 #
 # Changes made by dYdX to function name only.

def py_sign(msg_hash: int, priv_key: int, seed: Optional[int] = None) -> ECSignature:
    # Note: msg_hash must be smaller than 2**N_ELEMENT_BITS_ECDSA.
    # Message whose hash is >= 2**N_ELEMENT_BITS_ECDSA cannot be signed.
    # This happens with a very small probability.
    assert 0 <= msg_hash < 2**N_ELEMENT_BITS_ECDSA, 'Message not signable.'

    # Choose a valid k. In our version of ECDSA not every k value is valid,
    # and there is a negligible probability a drawn k cannot be used for signing.
    # This is why we have this loop.
    while True:
        k = generate_k_rfc6979(msg_hash, priv_key, seed)
        # Update seed for next iteration in case the value of k is bad.
        if seed is None:
            seed = 1
        else:
            seed += 1

        # Cannot fail because 0 < k < EC_ORDER and EC_ORDER is prime.
        x = ec_mult(k, EC_GEN, ALPHA, FIELD_PRIME)[0]

        # DIFF: in classic ECDSA, we take int(x) % n.
        r = int(x)
        if not (1 <= r < 2**N_ELEMENT_BITS_ECDSA):
            # Bad value. This fails with negligible probability.
            continue

        if (msg_hash + r * priv_key) % EC_ORDER == 0:
            # Bad value. This fails with negligible probability.
            continue

        w = div_mod(k, msg_hash + r * priv_key, EC_ORDER)
        if not (1 <= w < 2**N_ELEMENT_BITS_ECDSA):
            # Bad value. This fails with negligible probability.
            continue

        s = inv_mod_curve_size(w)
        return r, s


def mimic_ec_mult_air(m: int, point: ECPoint, shift_point: ECPoint) -> ECPoint:
    """
    Computes m * point + shift_point using the same steps like the AIR and throws an exception if
    and only if the AIR errors.
    """
    assert 0 < m < 2**N_ELEMENT_BITS_ECDSA
    partial_sum = shift_point
    for _ in range(N_ELEMENT_BITS_ECDSA):
        assert partial_sum[0] != point[0]
        if m & 1:
            partial_sum = ec_add(partial_sum, point, FIELD_PRIME)
        point = ec_double(point, ALPHA, FIELD_PRIME)
        m >>= 1
    assert m == 0
    return partial_sum


 # Starkware crypto functions implemented in Python.
 #
 # Copied from:
 # https://github.com/starkware-libs/starkex-resources/blob/0f08e6c55ad88c93499f71f2af4a2e7ae0185cdf/crypto/starkware/crypto/signature/signature.py
 #
 # Changes made by dYdX to function name only.

def py_verify(msg_hash: int, r: int, s: int, public_key: Union[int, ECPoint]) -> bool:
    # Compute w = s^-1 (mod EC_ORDER).
    assert 1 <= s < EC_ORDER, 's = %s' % s
    w = inv_mod_curve_size(s)

    # Preassumptions:
    # DIFF: in classic ECDSA, we assert 1 <= r, w <= EC_ORDER-1.
    # Since r, w < 2**N_ELEMENT_BITS_ECDSA < EC_ORDER, we only need to verify r, w != 0.
    assert 1 <= r < 2**N_ELEMENT_BITS_ECDSA, 'r = %s' % r
    assert 1 <= w < 2**N_ELEMENT_BITS_ECDSA, 'w = %s' % w
    assert 0 <= msg_hash < 2**N_ELEMENT_BITS_ECDSA, 'msg_hash = %s' % msg_hash

    if isinstance(public_key, int):
        # Only the x coordinate of the point is given, check the two possibilities for the y
        # coordinate.
        try:
            y = get_y_coordinate(public_key)
        except InvalidPublicKeyError:
            return False
        assert pow(y, 2, FIELD_PRIME) == (
            pow(public_key, 3, FIELD_PRIME) + ALPHA * public_key + BETA) % FIELD_PRIME
        return py_verify(msg_hash, r, s, (public_key, y)) or \
            py_verify(msg_hash, r, s, (public_key, (-y) % FIELD_PRIME))
    else:
        # The public key is provided as a point.
        # Verify it is on the curve.
        assert (public_key[1]**2 - (public_key[0]**3 + ALPHA *
                                    public_key[0] + BETA)) % FIELD_PRIME == 0

    # Signature validation.
    # DIFF: original formula is:
    # x = (w*msg_hash)*EC_GEN + (w*r)*public_key
    # While what we implement is:
    # x = w*(msg_hash*EC_GEN + r*public_key).
    # While both mathematically equivalent, one might error while the other doesn't,
    # given the current implementation.
    # This formula ensures that if the verification errors in our AIR, it errors here as well.
    try:
        zG = mimic_ec_mult_air(msg_hash, EC_GEN, MINUS_SHIFT_POINT)
        rQ = mimic_ec_mult_air(r, public_key, SHIFT_POINT)
        wB = mimic_ec_mult_air(w, ec_add(zG, rQ, FIELD_PRIME), SHIFT_POINT)
        x = ec_add(wB, MINUS_SHIFT_POINT, FIELD_PRIME)[0]
    except AssertionError:
        return False

    # DIFF: Here we drop the mod n from classic ECDSA.
    return r == x


#################
# Pedersen hash #
#################

 # Starkware crypto functions implemented in Python.
 #
 # Copied from:
 # https://github.com/starkware-libs/starkex-resources/blob/0f08e6c55ad88c93499f71f2af4a2e7ae0185cdf/crypto/starkware/crypto/signature/signature.py
 #
 # Changes made by dYdX to function name only.

def py_pedersen_hash(*elements: int) -> int:
    return pedersen_hash_as_point(*elements)[0]


def pedersen_hash_as_point(*elements: int) -> ECPoint:
    """
    Similar to pedersen_hash but also returns the y coordinate of the resulting EC point.
    This function is used for testing.
    """
    point = SHIFT_POINT
    for i, x in enumerate(elements):
        assert 0 <= x < FIELD_PRIME
        point_list = CONSTANT_POINTS[2 + i * N_ELEMENT_BITS_HASH:2 + (i + 1) * N_ELEMENT_BITS_HASH]
        assert len(point_list) == N_ELEMENT_BITS_HASH
        for pt in point_list:
            assert point[0] != pt[0], 'Unhashable input.'
            if x & 1:
                point = ec_add(point, pt, FIELD_PRIME)
            x >>= 1
        assert x == 0
    return point





# A type that represents a point (x,y) on an elliptic curve.
ECPoint = Tuple[int, int]


def pi_as_string(digits: int) -> str:
    """
    Returns pi as a string of decimal digits without the decimal point ("314...").
    """
    mpmath.mp.dps = digits  # Set number of digits.
    return '3' + str(mpmath.mp.pi)[2:]


def is_quad_residue(n: int, p: int) -> bool:
    """
    Returns True if n is a quadratic residue mod p.
    """
    return sympy.is_quad_residue(n, p)


def sqrt_mod(n: int, p: int) -> int:
    """
    Finds the minimum positive integer m such that (m*m) % p == n
    """
    return min(sympy.sqrt_mod(n, p, all_roots=True))


def div_mod(n: int, m: int, p: int) -> int:
    """
    Finds a nonnegative integer 0 <= x < p such that (m * x) % p == n
    """
    a, b, c = igcdex(m, p)
    assert c == 1
    return (n * a) % p


def ec_add(point1: ECPoint, point2: ECPoint, p: int) -> ECPoint:
    """
    Gets two points on an elliptic curve mod p and returns their sum.
    Assumes the points are given in affine form (x, y) and have different x coordinates.
    """
    assert (point1[0] - point2[0]) % p != 0
    m = div_mod(point1[1] - point2[1], point1[0] - point2[0], p)
    x = (m * m - point1[0] - point2[0]) % p
    y = (m * (point1[0] - x) - point1[1]) % p
    return x, y


def ec_neg(point: ECPoint, p: int) -> ECPoint:
    """
    Given a point (x,y) return (x, -y)
    """
    x, y = point
    return (x, (-y) % p)


def ec_double(point: ECPoint, alpha: int, p: int) -> ECPoint:
    """
    Doubles a point on an elliptic curve with the equation y^2 = x^3 + alpha*x + beta mod p.
    Assumes the point is given in affine form (x, y) and has y != 0.
    """
    assert point[1] % p != 0
    m = div_mod(3 * point[0] * point[0] + alpha, 2 * point[1], p)
    x = (m * m - 2 * point[0]) % p
    y = (m * (point[0] - x) - point[1]) % p
    return x, y


def ec_mult(m: int, point: ECPoint, alpha: int, p: int) -> ECPoint:
    """
    Multiplies by m a point on the elliptic curve with equation y^2 = x^3 + alpha*x + beta mod p.
    Assumes the point is given in affine form (x, y) and that 0 < m < order(point).
    """
    if m == 1:
        return point
    if m % 2 == 0:
        return ec_mult(m // 2, ec_double(point, alpha, p), alpha, p)
    return ec_add(ec_mult(m - 1, point, alpha, p), point, p)

def generate_query_path(url, params):
    entries = params.items()
    if not entries:
        return url

    paramsString = '&'.join('{key}={value}'.format(
        key=x[0], value=x[1]) for x in entries if x[1] is not None)
    if paramsString:
        return url + '?' + paramsString

    return url


def json_stringify(data):
    return json.dumps(data, separators=(',', ':'))


def random_client_id():
    return str(int(float(str(random.random())[2:])))


def generate_now_iso():
    return datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'


def iso_to_epoch_seconds(iso):
    return dp.parse(iso).timestamp()


def epoch_seconds_to_iso(epoch):
    return datetime.utcfromtimestamp(epoch).strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'


def remove_nones(original):
    return {k: v for k, v in original.items() if v is not None}

class DydxError(Exception):
    """Base error class for all exceptions raised in this library.
    Will never be raised naked; more specific subclasses of this exception will
    be raised when appropriate."""

class DydxApiError(DydxError):

    def __init__(self, response):
        self.status_code = response.status_code
        try:
            self.msg = response.json()
        except ValueError:
            self.msg = response.text
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return 'DydxApiError(status_code={}, response={})'.format(
            self.status_code,
            self.msg,
        )




# ------------ Ethereum Network IDs ------------
NETWORK_ID_MAINNET = 1
NETWORK_ID_GOERLI = 5


# ------------ Asset IDs ------------
COLLATERAL_ASSET_ID_BY_NETWORK_ID = {
    NETWORK_ID_MAINNET: int(
        '0x02893294412a4c8f915f75892b395ebbf6859ec246ec365c3b1f56f47c3a0a5d',
        16,
    ),
    NETWORK_ID_GOERLI: int(
        '0x03bda2b4764039f2df44a00a9cf1d1569a83f95406a983ce4beb95791c376008',
        16,
    ),
}

#########
# ECDSA #
#########


# ------------ Markets ------------
MARKET_BTC_USD = 'BTC-USD'
MARKET_ETH_USD = 'ETH-USD'
MARKET_LINK_USD = 'LINK-USD'
MARKET_AAVE_USD = 'AAVE-USD'
MARKET_UNI_USD = 'UNI-USD'
MARKET_SUSHI_USD = 'SUSHI-USD'
MARKET_SOL_USD = 'SOL-USD'
MARKET_YFI_USD = 'YFI-USD'
MARKET_ONEINCH_USD = '1INCH-USD'
MARKET_AVAX_USD = 'AVAX-USD'
MARKET_SNX_USD = 'SNX-USD'
MARKET_CRV_USD = 'CRV-USD'
MARKET_UMA_USD = 'UMA-USD'
MARKET_DOT_USD = 'DOT-USD'
MARKET_DOGE_USD = 'DOGE-USD'
MARKET_MATIC_USD = 'MATIC-USD'
MARKET_MKR_USD = 'MKR-USD'
MARKET_FIL_USD = 'FIL-USD'
MARKET_ADA_USD = 'ADA-USD'
MARKET_ATOM_USD = 'ATOM-USD'
MARKET_COMP_USD = 'COMP-USD'
MARKET_BCH_USD = 'BCH-USD'
MARKET_LTC_USD = 'LTC-USD'
MARKET_EOS_USD = 'EOS-USD'
MARKET_ALGO_USD = 'ALGO-USD'
MARKET_ZRX_USD = 'ZRX-USD'
MARKET_XMR_USD = 'XMR-USD'
MARKET_ZEC_USD = 'ZEC-USD'
MARKET_ENJ_USD = 'ENJ-USD'
MARKET_ETC_USD = 'ETC-USD'
MARKET_XLM_USD = 'XLM-USD'
MARKET_TRX_USD = 'TRX-USD'
MARKET_XTZ_USD = 'XTZ-USD'
MARKET_ICP_USD = 'ICP-USD'
MARKET_RUNE_USD = 'RUNE-USD'
MARKET_LUNA_USD = 'LUNA-USD'
MARKET_NEAR_USD = 'NEAR-USD'
MARKET_CELO_USD = 'CELO-USD'


# ------------ Assets ------------
ASSET_USDC = 'USDC'
ASSET_BTC = 'BTC'
ASSET_ETH = 'ETH'
ASSET_LINK = 'LINK'
ASSET_AAVE = 'AAVE'
ASSET_UNI = 'UNI'
ASSET_SUSHI = 'SUSHI'
ASSET_SOL = 'SOL'
ASSET_YFI = 'YFI'
ASSET_ONEINCH = '1INCH'
ASSET_AVAX = 'AVAX'
ASSET_SNX = 'SNX'
ASSET_CRV = 'CRV'
ASSET_UMA = 'UMA'
ASSET_DOT = 'DOT'
ASSET_DOGE = 'DOGE'
ASSET_MATIC = 'MATIC'
ASSET_MKR = 'MKR'
ASSET_FIL = 'FIL'
ASSET_ADA = 'ADA'
ASSET_ATOM = 'ATOM'
ASSET_COMP = 'COMP'
ASSET_BCH = 'BCH'
ASSET_LTC = 'LTC'
ASSET_EOS = 'EOS'
ASSET_ALGO = 'ALGO'
ASSET_ZRX = 'ZRX'
ASSET_XMR = 'XMR'
ASSET_ZEC = 'ZEC'
ASSET_ENJ = 'ENJ'
ASSET_ETC = 'ETC'
ASSET_XLM = 'XLM'
ASSET_TRX = 'TRX'
ASSET_XTZ = 'XTZ'
ASSET_ICP = 'ICP'
ASSET_RUNE = 'RUNE'
ASSET_LUNA = 'LUNA'
ASSET_NEAR = 'NEAR'
ASSET_CELO = 'CELO'
COLLATERAL_ASSET = ASSET_USDC


# ------------ Synthetic Assets by Market ------------
SYNTHETIC_ASSET_MAP = {
    MARKET_BTC_USD: ASSET_BTC,
    MARKET_ETH_USD: ASSET_ETH,
    MARKET_LINK_USD: ASSET_LINK,
    MARKET_AAVE_USD: ASSET_AAVE,
    MARKET_UNI_USD: ASSET_UNI,
    MARKET_SUSHI_USD: ASSET_SUSHI,
    MARKET_SOL_USD: ASSET_SOL,
    MARKET_YFI_USD: ASSET_YFI,
    MARKET_ONEINCH_USD: ASSET_ONEINCH,
    MARKET_AVAX_USD: ASSET_AVAX,
    MARKET_SNX_USD: ASSET_SNX,
    MARKET_CRV_USD: ASSET_CRV,
    MARKET_UMA_USD: ASSET_UMA,
    MARKET_DOT_USD: ASSET_DOT,
    MARKET_DOGE_USD: ASSET_DOGE,
    MARKET_MATIC_USD: ASSET_MATIC,
    MARKET_MKR_USD: ASSET_MKR,
    MARKET_FIL_USD: ASSET_FIL,
    MARKET_ADA_USD: ASSET_ADA,
    MARKET_ATOM_USD: ASSET_ATOM,
    MARKET_COMP_USD: ASSET_COMP,
    MARKET_BCH_USD: ASSET_BCH,
    MARKET_LTC_USD: ASSET_LTC,
    MARKET_EOS_USD: ASSET_EOS,
    MARKET_ALGO_USD: ASSET_ALGO,
    MARKET_ZRX_USD: ASSET_ZRX,
    MARKET_XMR_USD: ASSET_XMR,
    MARKET_ZEC_USD: ASSET_ZEC,
    MARKET_ENJ_USD: ASSET_ENJ,
    MARKET_ETC_USD: ASSET_ETC,
    MARKET_XLM_USD: ASSET_XLM,
    MARKET_TRX_USD: ASSET_TRX,
    MARKET_XTZ_USD: ASSET_XTZ,
    MARKET_ICP_USD: ASSET_ICP,
    MARKET_RUNE_USD: ASSET_RUNE,
    MARKET_LUNA_USD: ASSET_LUNA,
    MARKET_NEAR_USD: ASSET_NEAR,
    MARKET_CELO_USD: ASSET_CELO,
}


SYNTHETIC_ASSET_ID_MAP = {
    ASSET_BTC: int('0x4254432d3130000000000000000000', 16),
    ASSET_ETH: int('0x4554482d3900000000000000000000', 16),
    ASSET_LINK: int('0x4c494e4b2d37000000000000000000', 16),
    ASSET_AAVE: int('0x414156452d38000000000000000000', 16),
    ASSET_UNI: int('0x554e492d3700000000000000000000', 16),
    ASSET_SUSHI: int('0x53555348492d370000000000000000', 16),
    ASSET_SOL: int('0x534f4c2d3700000000000000000000', 16),
    ASSET_YFI: int('0x5946492d3130000000000000000000', 16),
    ASSET_ONEINCH: int('0x31494e43482d370000000000000000', 16),
    ASSET_AVAX: int('0x415641582d37000000000000000000', 16),
    ASSET_SNX: int('0x534e582d3700000000000000000000', 16),
    ASSET_CRV: int('0x4352562d3600000000000000000000', 16),
    ASSET_UMA: int('0x554d412d3700000000000000000000', 16),
    ASSET_DOT: int('0x444f542d3700000000000000000000', 16),
    ASSET_DOGE: int('0x444f47452d35000000000000000000', 16),
    ASSET_MATIC: int('0x4d415449432d360000000000000000', 16),
    ASSET_MKR: int('0x4d4b522d3900000000000000000000', 16),
    ASSET_FIL: int('0x46494c2d3700000000000000000000', 16),
    ASSET_ADA: int('0x4144412d3600000000000000000000', 16),
    ASSET_ATOM: int('0x41544f4d2d37000000000000000000', 16),
    ASSET_COMP: int('0x434f4d502d38000000000000000000', 16),
    ASSET_BCH: int('0x4243482d3800000000000000000000', 16),
    ASSET_LTC: int('0x4c54432d3800000000000000000000', 16),
    ASSET_EOS: int('0x454f532d3600000000000000000000', 16),
    ASSET_ALGO: int('0x414c474f2d36000000000000000000', 16),
    ASSET_ZRX: int('0x5a52582d3600000000000000000000', 16),
    ASSET_XMR: int('0x584d522d3800000000000000000000', 16),
    ASSET_ZEC: int('0x5a45432d3800000000000000000000', 16),
    ASSET_ENJ: int('0x454e4a2d3600000000000000000000', 16),
    ASSET_ETC: int('0x4554432d3700000000000000000000', 16),
    ASSET_XLM: int('0x584c4d2d3500000000000000000000', 16),
    ASSET_TRX: int('0x5452582d3400000000000000000000', 16),
    ASSET_XTZ: int('0x58545a2d3600000000000000000000', 16),
    ASSET_ICP: int('0x4943502d3700000000000000000000', 16),
    ASSET_RUNE: int('0x52554e452d36000000000000000000', 16),
    ASSET_LUNA: int('0x4c554e412d36000000000000000000', 16),
    ASSET_NEAR: int('0x4e4541522d36000000000000000000', 16),
    ASSET_CELO: int('0x43454c4f2d36000000000000000000', 16),
}

# ------------ Order Side ------------
ORDER_SIDE_BUY = 'BUY'
ORDER_SIDE_SELL = 'SELL'

# ------------ Time in Force Types ------------
TIME_IN_FORCE_GTT = 'GTT'
TIME_IN_FORCE_FOK = 'FOK'
TIME_IN_FORCE_IOC = 'IOC'

DECIMAL_CONTEXT_ROUND_DOWN = decimal.Context(rounding=decimal.ROUND_DOWN)
DECIMAL_CONTEXT_ROUND_UP = decimal.Context(rounding=decimal.ROUND_UP)


ONE_HOUR_IN_SECONDS = 60 * 60
ORDER_SIGNATURE_EXPIRATION_BUFFER_HOURS = 24 * 7  # Seven days.


ORDER_FIELD_BIT_LENGTHS = {
    "asset_id_synthetic": 128,
    "asset_id_collateral": 250,
    "asset_id_fee": 250,
    "quantums_amount": 64,
    "nonce": 32,
    "position_id": 64,
    "expiration_epoch_hours": 32,
}

"""Constants related to creating hashes of Starkware objects."""

ONE_HOUR_IN_SECONDS = 60 * 60
ORDER_SIGNATURE_EXPIRATION_BUFFER_HOURS = 24 * 7  # Seven days.

TRANSFER_PREFIX = 4
TRANSFER_PADDING_BITS = 81
CONDITIONAL_TRANSFER_PADDING_BITS = 81
CONDITIONAL_TRANSFER_PREFIX = 5
ORDER_PREFIX = 3
ORDER_PADDING_BITS = 17
WITHDRAWAL_PADDING_BITS = 49
WITHDRAWAL_PREFIX = 6

DEFAULT_API_TIMEOUT = 3000

CPP_LIB_PATH = None
OUT_BUFFER_SIZE = 251

PEDERSEN_HASH_POINT_FILENAME = os.path.join(
    os.path.dirname(__file__), 'pedersen_params.json')
PEDERSEN_PARAMS = json.load(open(PEDERSEN_HASH_POINT_FILENAME))



FIELD_PRIME = PEDERSEN_PARAMS['FIELD_PRIME']
FIELD_GEN = PEDERSEN_PARAMS['FIELD_GEN']
ALPHA = PEDERSEN_PARAMS['ALPHA']
BETA = PEDERSEN_PARAMS['BETA']
EC_ORDER = PEDERSEN_PARAMS['EC_ORDER']
CONSTANT_POINTS = PEDERSEN_PARAMS['CONSTANT_POINTS']
EC_ORDER = PEDERSEN_PARAMS['EC_ORDER']


N_ELEMENT_BITS_ECDSA = math.floor(math.log(FIELD_PRIME, 2))
assert N_ELEMENT_BITS_ECDSA == 251

DECIMAL_CTX_ROUND_DOWN = decimal.Context(rounding=decimal.ROUND_DOWN)
DECIMAL_CTX_ROUND_UP = decimal.Context(rounding=decimal.ROUND_UP)
DECIMAL_CTX_EXACT = decimal.Context(
    traps=[
        decimal.Inexact,
        decimal.DivisionByZero,
        decimal.InvalidOperation,
        decimal.Overflow,
    ],
)

ASSET_RESOLUTION = {
    ASSET_USDC: '1e6',
    ASSET_BTC: '1e10',
    ASSET_ETH: '1e9',
    ASSET_LINK: '1e7',
    ASSET_AAVE: '1e8',
    ASSET_UNI: '1e7',
    ASSET_SUSHI: '1e7',
    ASSET_SOL: '1e7',
    ASSET_YFI: '1e10',
    ASSET_ONEINCH: '1e7',
    ASSET_AVAX: '1e7',
    ASSET_SNX: '1e7',
    ASSET_CRV: '1e6',
    ASSET_UMA: '1e7',
    ASSET_DOT: '1e7',
    ASSET_DOGE: '1e5',
    ASSET_MATIC: '1e6',
    ASSET_MKR: '1e9',
    ASSET_FIL: '1e7',
    ASSET_ADA: '1e6',
    ASSET_ATOM: '1e7',
    ASSET_COMP: '1e8',
    ASSET_BCH: '1e8',
    ASSET_LTC: '1e8',
    ASSET_EOS: '1e6',
    ASSET_ALGO: '1e6',
    ASSET_ZRX: '1e6',
    ASSET_XMR: '1e8',
    ASSET_ZEC: '1e8',
    ASSET_ENJ: '1e6',
    ASSET_ETC: '1e7',
    ASSET_XLM: '1e5',
    ASSET_TRX: '1e4',
    ASSET_XTZ: '1e6',
    ASSET_ICP: '1e7',
    ASSET_RUNE: '1e6',
    ASSET_LUNA: '1e6',
    ASSET_NEAR: '1e6',
    ASSET_CELO: '1e6',
}

BIT_MASK_250 = (2 ** 250) - 1
NONCE_UPPER_BOUND_EXCLUSIVE = 1 << ORDER_FIELD_BIT_LENGTHS['nonce']

EC_GEN = CONSTANT_POINTS[1]
SHIFT_POINT = CONSTANT_POINTS[0]
MINUS_SHIFT_POINT = (SHIFT_POINT[0], FIELD_PRIME - SHIFT_POINT[1])

N_ELEMENT_BITS_HASH = FIELD_PRIME.bit_length()
assert N_ELEMENT_BITS_HASH == 252


StarkwareOrder = namedtuple(
    'StarkwareOrder',
    [
        'order_type',
        'asset_id_synthetic',
        'asset_id_collateral',
        'asset_id_fee',
        'quantums_amount_synthetic',
        'quantums_amount_collateral',
        'quantums_amount_fee',
        'is_buying_synthetic',
        'position_id',
        'nonce',
        'expiration_epoch_hours',
    ],
)