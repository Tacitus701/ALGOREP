all: requirement

requirement:
	pip3 install -r requirements.txt

run: clean
	python3 src/main.py

clean:
	rm -rf client disk
	mkdir disk client
