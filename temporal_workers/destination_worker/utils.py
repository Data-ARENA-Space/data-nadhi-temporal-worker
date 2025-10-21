import base64
import hashlib
import json
import re
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def decrypt_aes_gcm(b64: str, secret: str) -> str:
    """Decrypt a base64 string produced by encryptAesGcm in the server.

    - b64: base64 of iv(12) | tag(16) | ciphertext
    - secret: secret string; SHA-256(secret) is used as the 32-byte key
    """
    raw = base64.b64decode(b64)
    if len(raw) < 12 + 16:
        raise ValueError("Invalid encrypted payload: too short")

    iv = raw[:12]
    tag = raw[12:28]
    ciphertext = raw[28:]

    key = hashlib.sha256(secret.encode("utf-8")).digest()  # 32 bytes

    aesgcm = AESGCM(key)
    # cryptography expects ciphertext concatenated with tag
    plaintext = aesgcm.decrypt(iv, ciphertext + tag, None)

    return plaintext.decode("utf-8")


class StringTemplates:
    def __init__(self, template, data):
        self.template: str = template
        self.data: dict = data

    def escape_dollar(self):
        self.template = self.template.replace(r"\$", "__ESCAPED_DOLLAR__")

    def recreate_dollar(self):
        self.template = self.template.replace("__ESCAPED_DOLLAR__", "$")

    def get_value_from_path(self, path: str):
        """Safely resolve $.a.b.c from nested dict."""
        keys = path.strip("$.").split(".")
        current = self.data
        for k in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(k)
            if current is None:
                return None
        return current

    @staticmethod
    def stringify(data: Any):
        if isinstance(data, str):
            return data
        try:
            return json.dumps(data)
        except Exception:
            return str(data)

    def replacer(self, match):
        """Replace {{ $.path }} with value or leave as-is if unresolved."""
        full_match = match.group(0)  # exact text including spaces
        path = match.group(1).strip()
        value = self.get_value_from_path(path)
        return StringTemplates.stringify(value) if value is not None else full_match

    def render_template(self) -> str:
        self.escape_dollar()
        pattern = re.compile(r"{{\s*\$(.*?)\s*}}")
        result = pattern.sub(self.replacer, self.template)
        self.template = result
        self.recreate_dollar()

        return self.template
