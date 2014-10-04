#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

JAVAC=hadoop com.sun.tools.javac.Main
JAR=blockparty.jar

JSRC=	edu/rit/cs/*.java \

JCLASS=$(JSRC:.java=.class)

%.class: %.java
	$(JAVAC) $<

all: $(JCLASS)
	jar cvf $(JAR) $(JSRC)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
