#!/bin/bash

# curl -v  'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/CFHT/1681917o.fits.fz
# ?cutout=[0]&cutout=[2][200:300,200:300]&cutout=[3][300:400,300:400]&cutout=[4][100:200,100:200]'

CUT0="%5B0%5D"
CUT1="%5B1%5D%5B100%3A200%2C100%3A200%5D"
CUT2="%5B2%5D%5B200%3A300%2C200%3A300%5D"
CUT3="%5B3%5D%5B300%3A400%2C300%3A400%5D"

CUTS="fo=sample-mef.fits&cutout=${CUT1}&cutout=${CUT2}&cutout=${CUT3}"

curl -v --location -O -J https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/CFHT/1681917o.fits.fz?${CUTS}
