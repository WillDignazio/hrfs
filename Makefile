#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

CLASSPATH=$(shell hadoop classpath)
JAVAC=javac #hadoop com.sun.tools.javac.Main
JARC=jar

JAR=	bpfs.jar
JSRC=	edu/rit/cs/Bpfs.java			\
	edu/rit/cs/BpfsConfiguration.java	\
	edu/rit/cs/BpfsKeys.java		\
	edu/rit/cs/BpfsNode.java		\
	edu/rit/cs/BpfsMapper.java		\
	edu/rit/cs/BpfsReducer.java		\
	edu/rit/cs/BpfsRPC.java			\
	edu/rit/cs/examples/BpfsClient.java	\
	edu/rit/cs/examples/BpfsNodeClient.java	\
	edu/rit/cs/examples/BpfsMapTest.java	\

JCLASS=	$(JSRC:.java=.class) \
	edu/rit/cs/*.class

%.class: %.java
	$(JAVAC) -classpath $(CLASSPATH):. $<

all: $(JCLASS)
	$(JARC) -cvf $(JAR) $(JCLASS)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
