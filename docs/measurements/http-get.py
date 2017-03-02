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
c.setopt(c.USERAGENT, 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/55.0.2883.87 Chrome/55.0.2883.87 Safari/537.36')

#execute request
failure=0
try:
    c.perform()
except:
    #e = sys.exc_info()[0] #catch all errors
    if c.errstr() != "GnuTLS recv error (-110): The TLS connection was non-properly terminated.":
        failure=1

#parse for failure
if failure:
    print('{', end='')
    print('"error":true', end='')
    print(',"error_message":"%s"' % c.errstr(), end='')
    print('}', end='', flush=True)
else:
    #print body
    body = buffer.getvalue()
    #print(body.decode('iso-8859-1'))

    #print information
    print('{', end='')
    print('"error":false,', end='')
    print('"effective_url":"%s",' % c.getinfo(c.EFFECTIVE_URL), end='')
    print('"domain_ip":"%s",' % c.getinfo(c.PRIMARY_IP), end='')
    print('"domain_port":%d,' % c.getinfo(c.PRIMARY_PORT), end='')
    print('"application_layer_latency":%f,' % c.getinfo(c.TOTAL_TIME), end='')
    print('"redirect_count":%d,' % c.getinfo(c.REDIRECT_COUNT), end='')
    print('"http_status_code":%d,' % c.getinfo(c.RESPONSE_CODE), end='')
    print('"request_size":%d,' % c.getinfo(c.REQUEST_SIZE), end='')
    print('"content_size":%d' % len(body), end='')
    print('}', end='', flush=True)

#close curl handle
c.close()
