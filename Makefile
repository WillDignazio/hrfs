#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

CLASSPATH=$(shell hadoop classpath)
JAVAC=javac -Xlint #hadoop com.sun.tools.javac.Main
JARC=jar

JAR=	hrfs.jar
JSRC=	edu/rit/cs/HrfsConfiguration.java	\
	edu/rit/cs/HrfsKeys.java		\
	edu/rit/cs/HrfsRPC.java			\
	edu/rit/cs/HrfsRing.java		\
	edu/rit/cs/HrfsSession.java		\
	edu/rit/cs/Hrfs.java			\
	edu/rit/cs/cluster/ClusterLock.java	\
	edu/rit/cs/cluster/RingMonitor.java	\
	edu/rit/cs/cluster/ClusterAgent.java	\
	edu/rit/cs/node/NodeWriter.java		\
	edu/rit/cs/node/HrfsNode.java		\
	edu/rit/cs/examples/HrfsClient.java	\
	edu/rit/cs/examples/HrfsNodeClient.java	\

JCLASS=	$(JSRC:.java=.class)		\
	edu/rit/cs/*.class		\
	edu/rit/cs/cluster/*.class	\
	edu/rit/cs/examples/*.class	\

%.class: %.java
	$(JAVAC) -classpath $(CLASSPATH):. $<

all: $(JCLASS)
	$(JARC) -cvf $(JAR) $(JCLASS)

clean:
	rm -f $(JCLASS)
	rm -f edu/rit/cs/*.class
	rm -f edu/rit/cs/cluster/*.class
	rm -f edu/rit/cs/examples/*.class
	rm -f $(JAR)
