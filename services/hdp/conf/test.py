#! /usr/bin/python
from subprocess import call


# mvn integration-test -Pintegration -DargLine="-Dwhirr.test.provider=aws-ec2 -Dwhirr.test.identity=AKIAJNX24OYZ7QLYHXYA -Dwhirr.test.credential=OpjCRDHcnD4Nfm7+Nb/TvLv2I4Y+UNrG5yk6onBh -Dwhirr.test.image-id=us-west-2/ami-3659d706"
command = "mvn"
profile="integration"
identity="AKIAJNX24OYZ7QLYHXYA"
credential="OpjCRDHcnD4Nfm7+Nb/TvLv2I4Y+UNrG5yk6onBh"
image="us-west-2/ami-3659d706"
argLine = "-Dwhirr.test.provider=aws-ec2 -Dwhirr.test.identity="+identity+" -Dwhirr.test.credential="+credential+" -Dwhirr.test.image-id="+image
fullargline= "-DargLine=" + argLine + ""
print fullargline
call([command,
      "integration-test",
      "-P"+profile,
      fullargline
      ])
#      ], stderr = subprocess.STDOUT)




