#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

CLASSPATH=$(shell hadoop classpath)
JAVAC=javac #hadoop com.sun.tools.javac.Main
JARC=jar

JAR=	blockparty.jar
JSRC=	edu/rit/cs/Bpfs.java			\

JCLASS=$(JSRC:.java=.class)

%.class: %.java
	$(JAVAC) -classpath $(CLASSPATH):. $<

all: $(JCLASS)
	$(JARC) -cvf $(JAR) $(JCLASS)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
