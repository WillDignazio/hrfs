#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

CLASSPATH=$(shell hadoop classpath)
JAVAC=javac #hadoop com.sun.tools.javac.Main
JARC=jar

JAR=	blockparty.jar
JSRC=	edu/rit/cs/Debugger.java		\
	edu/rit/cs/BlockPartyClient.java	\
	edu/rit/cs/BlockPartyReader.java	\
	edu/rit/cs/FileMetadata.java		\

JCLASS=$(JSRC:.java=.class)

%.class: %.java
	$(JAVAC) -classpath $(CLASSPATH):. $<

all: $(JCLASS)
	$(JARC) -cvf $(JAR) $(JCLASS)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
