#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

CLASSPATH=$(shell hadoop classpath)
JAVAC=javac #hadoop com.sun.tools.javac.Main
JARC=jar

JAR=	hrfs.jar
JSRC=	edu/rit/cs/Hrfs.java			\
	edu/rit/cs/HrfsConfiguration.java	\
	edu/rit/cs/HrfsKeys.java		\
	edu/rit/cs/HrfsNode.java		\
	edu/rit/cs/HrfsRPC.java			\
	edu/rit/cs/NodeWriter.java		\
	edu/rit/cs/MetadataBlock.java		\
	edu/rit/cs/examples/HrfsClient.java	\
	edu/rit/cs/examples/HrfsNodeClient.java	\

JCLASS=	$(JSRC:.java=.class) \
	edu/rit/cs/*.class

%.class: %.java
	$(JAVAC) -classpath $(CLASSPATH):. $<

all: $(JCLASS)
	$(JARC) -cvf $(JAR) $(JCLASS)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
