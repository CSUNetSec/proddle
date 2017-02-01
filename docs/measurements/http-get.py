import pycurl
import sys

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

def main():
    prefixes = ['', 'www.']
    errors = []

    for prefix in prefixes:
        #create curl request
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, prefix + sys.argv[1])
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.TIMEOUT, 120)

        #execute request
        try:
            c.perform()

            #print body
            #body = buffer.getvalue()
            #print(body.decode('iso-8859-1'))

            #print information
            print('{', end='')
            print('"Error":false,', end='')
            print('"AttemptUrl":"%s",' % (prefix + sys.argv[1]), end='')
            print('"DomainUrl":"%s",' % c.getinfo(c.EFFECTIVE_URL), end='')
            print('"DomainIp":"%s",' % c.getinfo(c.PRIMARY_IP), end='')
            print('"DomainPort":%d,' % c.getinfo(c.PRIMARY_PORT), end='')
            print('"LocalIp":"%s",' % c.getinfo(c.LOCAL_IP), end='')
            print('"ApplicationLayerLatency":%f,' % c.getinfo(c.TOTAL_TIME), end='')
            print('"RedirectCount":%d,' % c.getinfo(c.REDIRECT_COUNT), end='')
            print('"HttpStatusCode":%d,' % c.getinfo(c.RESPONSE_CODE), end='')
            print('"RequestSize":%d,' % c.getinfo(c.REQUEST_SIZE), end='')
            print('"ContentSize":%d' % c.getinfo(c.CONTENT_LENGTH_DOWNLOAD), end='')
            print('}', end='', flush=True)

            return
        except:
            e = sys.exc_info()[0] #catch all errors
            errors.append('"' + c.errstr() + '"')

        #close curl handle
        c.close()

    print('{', end='')
    print('"Error":true,', end='')
    print('"ErrorMessages":[%s' % ','.join(errors), end='')
    print(']}', end='', flush=True)

if __name__ == '__main__':
    main()
