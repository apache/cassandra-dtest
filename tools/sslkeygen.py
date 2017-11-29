import os
import os.path
import tempfile
import subprocess


def generate_credentials(ip, cakeystore=None, cacert=None):

    tmpdir = tempfile.mkdtemp()

    if not cakeystore:
        cakeystore = generate_cakeypair(tmpdir, 'ca')
    if not cacert:
        cacert = generate_cert(tmpdir, "ca", cakeystore)

    # create keystore with new private key
    name = "ip" + ip
    jkeystore = generate_ipkeypair(tmpdir, name, ip)

    # create signed cert
    csr = generate_sign_request(tmpdir, name, jkeystore, ['-ext', 'san=ip:' + ip])
    cert = sign_request(tmpdir, "ca", cakeystore, csr, ['-ext', 'san=ip:' + ip])

    # import cert chain into keystore
    import_cert(tmpdir, "ca", cacert, jkeystore)
    import_cert(tmpdir, name, cert, jkeystore)

    return SecurityCredentials(jkeystore, cert, cakeystore, cacert)


def generate_cakeypair(dir, name):
    return generate_keypair(dir, name, name, ['-ext', 'bc:c'])


def generate_ipkeypair(dir, name, ip):
    return generate_keypair(dir, name, ip, ['-ext', 'san=ip:' + ip])


def generate_dnskeypair(dir, name, hostname):
    return generate_keypair(dir, name, hostname, ['-ext', 'san=dns:' + hostname])


def generate_keypair(dir, name, cn, opts):
    kspath = os.path.join(dir, name + '.keystore')
    return _exec_keytool(dir, kspath, ['-alias', name, '-genkeypair', '-keyalg', 'RSA', '-dname',
                                       "cn={}, ou=cassandra, o=apache.org, c=US".format(cn), '-keypass', 'cassandra'] + opts)


def generate_cert(dir, name, keystore, opts=[]):
    fn = os.path.join(dir, name + '.pem')
    _exec_keytool(dir, keystore, ['-alias', name, '-exportcert', '-rfc', '-file', fn] + opts)
    return fn


def generate_sign_request(dir, name, keystore, opts=[]):
    fn = os.path.join(dir, name + '.csr')
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-certreq', '-file', fn] + opts)
    return fn


def sign_request(dir, name, keystore, csr, opts=[]):
    fnout = os.path.splitext(csr)[0] + '.pem'
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-gencert',
                                  '-rfc', '-infile', csr, '-outfile', fnout] + opts)
    return fnout


def import_cert(dir, name, cert, keystore, opts=[]):
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-importcert', '-noprompt', '-file', cert] + opts)
    return cert


def _exec_keytool(dir, keystore, opts):
    args = ['keytool', '-keystore', keystore, '-storepass', 'cassandra', '-deststoretype', 'pkcs12'] + opts
    subprocess.check_call(args)
    return keystore


class SecurityCredentials():

    def __init__(self, keystore, cert, cakeystore, cacert):
        self.keystore = keystore
        self.cert = cert
        self.cakeystore = cakeystore
        self.cacert = cacert
        self.basedir = os.path.dirname(self.keystore)

    def __str__(self):
        return "keystore: {}, cert: {}, cakeystore: {}, cacert: {}".format(
               self.keystore, self.cert, self.cakeystore, self.cacert)
