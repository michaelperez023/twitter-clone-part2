open System.Security.Cryptography

// RSA encryption function
let RsaEncrypt (data : byte[]) (key : RSAParameters) (padding : bool) : byte[] =
    try
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportParameters(key)
        rsa.Encrypt(data, padding)
    with
    | :? CryptographicException as e ->
        printfn "%A" e.Message
        Array.empty

// RSA decryption function
let RsaDecrypt (data : byte[]) (key : RSAParameters) (padding : bool) : byte[] =
    try
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportParameters(key)
        rsa.Decrypt(data, padding)
    with
    | :? CryptographicException as e ->
        printfn "%A" e.Message
        Array.empty  

// Example
// ========================
let rsa = new RSACryptoServiceProvider(2048)
let publicPrivateKeyXML = rsa.ToXmlString(true)
rsa.FromXmlString(publicPrivateKeyXML)
let publicKey = rsa.ExportParameters(false)
let privateKey = rsa.ExportParameters(true)

// First round
let plainText = "hello world"
let plainTextArray = System.Text.Encoding.ASCII.GetBytes(plainText)
let cipherArray = RsaEncrypt plainTextArray (publicKey) false
let cipherText = System.Text.Encoding.ASCII.GetString(cipherArray)

printfn "\nFirst Round c: %A\n" cipherText

let decryptedTextArray = RsaDecrypt (cipherArray) (privateKey) false
let decryptedText = System.Text.Encoding.ASCII.GetString(decryptedTextArray)

printfn "\nFirst Round m: %A\n" decryptedText