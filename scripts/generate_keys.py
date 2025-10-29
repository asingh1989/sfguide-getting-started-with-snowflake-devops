#!/usr/bin/env python3
"""
Generate RSA key pair for Snowflake authentication
This script generates the private and public keys needed for Snowflake key-pair authentication
"""

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import os

def generate_rsa_keypair():
    """Generate RSA key pair for Snowflake authentication"""
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    
    # Get public key
    public_key = private_key.public_key()
    
    # Serialize private key (no encryption for GitHub Actions)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    # Serialize public key
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    
    return private_pem, public_pem

def main():
    """Main function to generate and save keys"""
    print("🔑 Generating RSA key pair for Snowflake authentication...")
    
    try:
        private_key, public_key = generate_rsa_keypair()
        
        # Create .snowflake directory if it doesn't exist
        os.makedirs('.snowflake', exist_ok=True)
        
        # Save private key
        with open('.snowflake/rsa_key.pem', 'wb') as f:
            f.write(private_key)
        
        # Save public key
        with open('.snowflake/rsa_key.pub', 'wb') as f:
            f.write(public_key)
        
        print("✅ Keys generated successfully!")
        print("\n📁 Files created:")
        print("   .snowflake/rsa_key.pem (private key - for GitHub Secrets)")
        print("   .snowflake/rsa_key.pub (public key - for Snowflake user)")
        
        print("\n🔐 Private Key for GitHub Secret (SNOWFLAKE_PRIVATE_KEY):")
        print("=" * 60)
        print(private_key.decode('utf-8'))
        print("=" * 60)
        
        print("\n🔑 Public Key for Snowflake User:")
        print("=" * 40)
        print(public_key.decode('utf-8'))
        print("=" * 40)
        
        print("\n📋 Next Steps:")
        print("1. Copy the private key above to GitHub Secrets as 'SNOWFLAKE_PRIVATE_KEY'")
        print("2. Add the public key to your Snowflake user with:")
        print("   ALTER USER asingh92 SET RSA_PUBLIC_KEY='<public_key_content>';")
        print("3. Update your .snowflake/config.toml to use key-pair authentication")
        
    except ImportError:
        print("❌ Error: 'cryptography' package not found")
        print("📦 Install it with: pip install cryptography")
    except Exception as e:
        print(f"❌ Error generating keys: {str(e)}")

if __name__ == "__main__":
    main()