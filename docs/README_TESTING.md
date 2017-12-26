# Testing

Prom can be tested using [pyenv](https://github.com/pyenv/pyenv) and [tox](https://github.com/tox-dev/tox). 

The current `tox.ini` file expects python 2.7 and 3.6 versions to be globally available. You can do this by running (after pyenv and tox are installed):

    $ pyenv install 2.7.14
    $ pyenv install 3.6.3
    $ pyenv global 2.7.14 3.6.3
    
You can verify these are now available:

    $ python --version
    Python 2.7.14
    $ python3.6 --version
    Python 3.6.3
    
And then you can run `tox` to test (from the prom repository direcotry):

	$ tox

You can reset your environment to the default python by running:

	$ pyenv global system