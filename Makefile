all: requirement

requirement:
	pip3 install -r requirements.txt

run: clean
	mkdir disk client
	python3 src/main.py

clean:
	rm -rf client disk
