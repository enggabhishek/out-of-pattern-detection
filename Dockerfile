FROM quay.io/astronomer/astro-runtime:11.6.0
ARG TESTNAME
ENV TESTNAME=${TESTNAME}
