
from cluster_pack.skein import skein_launcher
import sys
sys.modules['cluster_pack.skein.yarn_launcher'] = skein_launcher
