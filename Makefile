all: requirement

requirement:
	pip install -r requirements.txt

run: clean
	python3 src/main.py

clean:
	rm -f client/* disk/*
