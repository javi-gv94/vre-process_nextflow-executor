"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from __future__ import print_function

import sys
import os
import configparser
import subprocess
import tempfile
import json
import fnmatch
import tarfile
import shutil
import io

from utils import logger

try:
    if hasattr(sys, '_run_from_cmdl') is True:
        raise ImportError
    from pycompss.api.parameter import FILE_IN, FILE_OUT
    from pycompss.api.task import task
    from pycompss.api.api import compss_wait_on
except ImportError:
#    logger.warn("[Warning] Cannot import \"pycompss\" API packages.")
#    logger.warn("          Using mock decorators.")

    from utils.dummy_pycompss import FILE_IN, FILE_OUT # pylint: disable=ungrouped-imports
    from utils.dummy_pycompss import task # pylint: disable=ungrouped-imports
    from utils.dummy_pycompss import compss_wait_on # pylint: disable=ungrouped-imports

from basic_modules.tool import Tool
from basic_modules.metadata import Metadata

# ------------------------------------------------------------------------------

class VRE_NF_RUNNER(Tool):
    """
    Tool for writing to a file
    """
    DEFAULT_NXF_IMAGE='nextflow/nextflow'
    DEFAULT_NXF_VERSION='19.04.1'

    def __init__(self, configuration=None):
        """
        Init function
        """
        logger.info("OpenEBench VRE Nexflow pipeline runner")
        Tool.__init__(self)

        local_config = configparser.ConfigParser()
        local_config.read(sys.argv[0] + '.ini')
        # Setup parameters
        self.nxf_image = local_config.get('nextflow','docker_image')  if local_config.has_option('nextflow','docker_image') else self.DEFAULT_NXF_IMAGE
        self.nxf_version = local_config.get('nextflow','version')  if local_config.has_option('nextflow','version') else self.DEFAULT_NXF_VERSION
        
        # Now, we have to assure the nextflow image is already here
        docker_tag = self.nxf_image+':'+self.nxf_version
        checkimage_params = [
            "docker","images","--format","{{.ID}}\t{{.Tag}}",docker_tag
        ]
        
        checkimage_stdout = io.StringIO()
        checkimage_stderr = io.StringIO()
        retval = subprocess.call(checkimage_params,stdout=checkimage_stdout,stderr=checkimage_stderr)
        if retval == 0:
            if len(checkimage_stdout.getvalue()) == 0:
                # The image is not here yet
                pullimage_params = [
                    "docker","pull",docker_tag
                ]
                pullimage_stdout = io.StringIO()
                pullimage_stderr = io.StringIO()
                retval = subprocess.call(pullimage_params,stdout=pullimage_stdout,stderr=pullimage_stderr)
                if retval != 0:
                    # It failed!
                    errstr = "ERROR: VRE Nextflow Runner failed while pulling Nextflow image. Tag: {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(docker_tag,pullimage_stdout.getvalue(),pullimage_stderr.getvalue())
                    logger.fatal(errstr)
                    raise Exception(errstr)
        else:
            errstr = "ERROR: VRE Nextflow Runner failed while checking Nextflow image. Tag: {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(docker_tag,checkimage_stdout.getvalue(),checkimage_stderr.getvalue())
            logger.fatal(errstr)
            raise Exception(errstr)
        
        if configuration is None:
            configuration = {}

        self.configuration.update(configuration)

    @task(returns=bool, genes_loc=FILE_IN, metrics_ref_dir_loc=FILE_IN, assess_dir_loc=FILE_IN, public_ref_dir_loc=FILE_IN, metrics_loc=FILE_OUT, tar_view_loc=FILE_OUT, isModifier=False)
    def validate_and_assess(self, genes_loc, metrics_ref_dir_loc, assess_dir_loc, public_ref_dir_loc, metrics_loc, tar_view_loc):  # pylint: disable=no-self-use
        participant_id = self.configuration['participant_id']
        cancer_types = self.configuration['cancer_type']
        
        inputDir = os.path.dirname(genes_loc)
        inputBasename = os.path.basename(genes_loc)
        
        # Value needed to compose the Nextflow docker call
        uid = str(os.getuid())
        
        # TODO workdir
        workdir = 'TODO'
        
        # Directories required by Nextflow in a Docker
        homedir = os.path.expanduser("~")
        nxf_assets_dir = os.path.join(homedir,".nextflow","assets")
        if not os.path.exists(nxf_assets_dir):
            os.makedirs(nxf_assets_dir)
        
        retval_stage = 'validation'
        validation_params = [
            "docker", "run", "--rm", "--net", "host",
            "-e", "USER",
            "-e", "HOME="+homedir,
            "-e", "NXF_ASSETS=$NXF_ASSETS",
            "-e", "NXF_USRMAP="+uid,
            "-e", "NXF_DOCKER_OPTS=-u "+uid+" -e HOME="+homedir,
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-v", homedir+":"+homedir+":ro,Z",
            "-v", nxf_assets_dir+":"+nxf_assets_dir+":Z",
            "-v", workdir+":"+workdir+":Z",
            "-w", workdir,
            self.nxf_image+":"+self.nxf_version,
            "nextflow", "${args[@]}", "-with-docker"
        ]
        
        #print("DEBUG: "+'  '.join(validation_params),file=sys.stderr)
        retval = subprocess.call(validation_params)
        
        resultsDir = None
        resultsTarDir = None
        if retval == 0:
                retval_stage = 'metrics'
                resultsDir = tempfile.mkdtemp()
                resultsTarDir = tempfile.mkdtemp()
                metrics_params = [
                        "docker","run","--rm","-u", uid,
                        '-v',inputDir + ":/app/input:ro",
                        '-v',metrics_ref_dir_loc+":/app/metrics:ro",
                        '-v',resultsDir+":/app/results:rw",
                        "tcga_metrics:" + tag,
                        '-i',"/app/input/"+inputBasename,'-m','/app/metrics/','-p',participant_id,'-o','/app/results/',
                        '-c'
                ]
                metrics_params.extend(cancer_types)
                
                retval = subprocess.call(metrics_params)
                if retval == 0:
                        retval_stage = 'assessment'
                        retval = subprocess.call([
                                "docker","run","--rm","-u", uid,
                                '-v',assess_dir_loc+":/app/assess:ro",
                                '-v',resultsDir+":/app/results:rw",
                                '-v',resultsTarDir+":/app/resultsTar:rw",
                                "tcga_assessment:" + tag,
                                '-b',"/app/assess/",'-p','/app/results/','-o','/app/resultsTar/'
                        ])
        
        try:
                if retval == 0:
                        # Create the MuG/VRE metrics file
                        metricsArray = []
                        for metrics_file in os.listdir(resultsDir):
                                abs_metrics_file = os.path.join(resultsDir, metrics_file)
                                if fnmatch.fnmatch(metrics_file,"*.json") and os.path.isfile(abs_metrics_file):
                                        with io.open(abs_metrics_file,mode='r',encoding="utf-8") as f:
                                                metrics = json.load(f)
                                                metricsArray.append(metrics)
                        
                        with io.open(metrics_loc, mode='w', encoding="utf-8") as f:
                                jdata = json.dumps(metricsArray, sort_keys=True, indent=4, separators=(',', ': '))
                                f.write(unicode(jdata,"utf-8"))
                        
                        # And create the MuG/VRE tar file
                        with tarfile.open(tar_view_loc,mode='w:gz',bufsize=1024*1024) as tar:
                                tar.add(resultsTarDir,arcname='data',recursive=True)
                else:
                        logger.fatal("ERROR: TCGA CD evaluation failed, in step "+retval_stage)
                        raise Exception("ERROR: TCGA CD evaluation failed, in step "+retval_stage)
                        return False
        except IOError as error:
                logger.fatal("I/O error({0}): {1}".format(error.errno, error.strerror))
                return False
        finally:
                # Cleaning up in any case
                if resultsDir is not None:
                        shutil.rmtree(resultsDir)
                if resultsTarDir is not None:
                        shutil.rmtree(resultsTarDir)

        return True

    def run(self, input_files, input_metadata, output_files):
        """
        The main function to run the compute_metrics tool

        Parameters
        ----------
        input_files : dict
            List of input files - In this case there are no input files required
        input_metadata: dict
            Matching metadata for each of the files, plus any additional data
        output_files : dict
            List of the output files that are to be generated

        Returns
        -------
        output_files : dict
            List of files with a single entry.
        output_metadata : dict
            List of matching metadata for the returned files
        """
        project_path = self.configuration.get('project','.')
        participant_id = self.configuration['participant_id']
        
        metrics_path = output_files.get("metrics")
        if metrics_path is None:
                metrics_path = os.path.join(project_path,participant_id+'.json')
        metrics_path = os.path.abspath(metrics_path)
        output_files['metrics'] = metrics_path
        
        tar_view_path = output_files.get("tar_view")
        if tar_view_path is None:
                tar_view_path = os.path.join(project_path,participant_id+'.tar.gz')
        tar_view_path = os.path.abspath(tar_view_path)
        output_files['tar_view'] = tar_view_path
        
        results = self.validate_and_assess(
            os.path.abspath(input_files["genes"]),
            os.path.abspath(input_files['metrics_ref_datasets']),
            os.path.abspath(input_files['assessment_datasets']),
            os.path.abspath(input_files['public_ref']),
            metrics_path,
            tar_view_path
        )
        results = compss_wait_on(results)

        if results is False:
            logger.fatal("VRE NF RUNNER pipeline failed. See logs")
            raise Exception("VRE NF RUNNER pipeline failed. See logs")
            return {}, {}
        
        # BEWARE: Order DOES MATTER when there is a dependency from one output on another
        output_metadata = {
            "metrics": Metadata(
                # These ones are already known by the platform
                # so comment them by now
                data_type="metrics",
                file_type="TXT",
                file_path=metrics_path,
                # Reference and golden data set paths should also be here
                sources=[input_metadata["genes"].file_path],
                meta_data={
                    "tool": "VRE_NF_RUNNER"
                }
            ),
            "tar_view": Metadata(
                # These ones are already known by the platform
                # so comment them by now
                data_type="tool_statistics",
                file_type="TAR",
                file_path=tar_view_path,
                # Reference and golden data set paths should also be here
                sources=[metrics_path],
                meta_data={
                    "tool": "VRE_NF_RUNNER"
                }
            ),
        }

        return (output_files, output_metadata)
