#
# Copyright (C) 2014 Will Dignazio
#
.PHONY: all clean src

JAR=blockparty.jar

JSRC=	edu/rit/cs/*.java \

JCLASS=$(JSRC:.java=.class)

%.class: %.java
	javac $<

all: $(JCLASS)
	jar cvf $(JAR) $(JSRC)

clean:
	rm -f $(JCLASS)
	rm -f $(JAR)
