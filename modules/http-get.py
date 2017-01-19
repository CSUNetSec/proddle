import pycurl
import sys

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

#create curl request
buffer = BytesIO()
c = pycurl.Curl()
c.setopt(c.URL, sys.argv[1])
c.setopt(c.WRITEDATA, buffer)
c.setopt(c.FOLLOWLOCATION, True)
c.setopt(c.TIMEOUT, 120)

#execute request
try:
    c.perform()

    #print body
    body = buffer.getvalue()
    #print(body.decode('iso-8859-1'))

    #print information
    print('{', end='')
    print('"Error":false,', end='')
    print('"HttpStatusCode":%d,' % c.getinfo(c.RESPONSE_CODE), end='')
    print('"ApplicationLayerLatency":%f,' % c.getinfo(c.TOTAL_TIME), end='')
    print('"PrimaryIP":"%s",' % c.getinfo(c.PRIMARY_IP), end='')
    print('"RedirectCount":%d' % c.getinfo(c.REDIRECT_COUNT), end='')
    print('}', end='', flush=True)
except:
    e = sys.exc_info()[0] #catch all errors
    print('{', end='')
    print('"Error":true,', end='')
    print('"ErrorMessage":"%s"' % c.errstr(), end='')
    print('}', end='', flush=True)

#close curl handle
c.close()
