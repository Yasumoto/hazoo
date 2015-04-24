from .. import commands

from twitter.common import app
from twitter.common.log import LogOptions


app.register_commands_from(commands)


def main():
  app.help()



LogOptions.set_stderr_log_level('google:INFO')
LogOptions.disable_disk_logging()
app.set_name('hazoo')



def proxy_main():
  app.main()