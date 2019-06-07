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

import hashlib

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

import tempfile

# ------------------------------------------------------------------------------

class WF_RUNNER(Tool):
    """
    Tool for writing to a file
    """
    DEFAULT_NXF_IMAGE='nextflow/nextflow'
    DEFAULT_NXF_VERSION='19.04.1'
    DEFAULT_WF_BASEDIR='WF-checkouts'
    
    DEFAULT_DOCKER_CMD='docker'
    DEFAULT_GIT_CMD='git'
    
    MASKED_KEYS = { 'execution', 'project', 'description', 'nextflow_repo_uri', 'nextflow_repo_tag' }

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
        
        self.wf_basedir = os.path.abspath(local_config.get('workflows','basedir')  if local_config.has_option('workflows','basedir') else self.DEFAULT_WF_BASEDIR)
        
        # Where the external commands should be located
        self.docker_cmd = local_config.get('defaults','docker_cmd')  if local_config.has_option('defaults','docker_cmd') else self.DEFAULT_DOCKER_CMD
        self.git_cmd = local_config.get('defaults','git_cmd')  if local_config.has_option('defaults','git_cmd') else self.DEFAULT_GIT_CMD
        
        # Now, we have to assure the nextflow image is already here
        docker_tag = self.nxf_image+':'+self.nxf_version
        checkimage_params = [
            self.docker_cmd,"images","--format","{{.ID}}\t{{.Tag}}",docker_tag
        ]
        
        with tempfile.NamedTemporaryFile() as checkimage_stdout:
            with tempfile.NamedTemporaryFile() as checkimage_stderr:
                retval = subprocess.call(checkimage_params,stdout=checkimage_stdout,stderr=checkimage_stderr)

                if retval != 0:
                    # Reading the output and error for the report
                    with open(checkimage_stdout.name,"r") as c_stF:
                        checkimage_stdout_v = c_stF.read()
                    with open(checkimage_stderr.name,"r") as c_stF:
                        checkimage_stderr_v = c_stF.read()
                    
                    errstr = "ERROR: VRE Nextflow Runner failed while checking Nextflow image. Tag: {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(docker_tag,checkimage_stdout_v,checkimage_stderr_v)
                    logger.fatal(errstr)
                    raise Exception(errstr)
            
            do_pull_image = os.path.getsize(checkimage_stdout.name) == 0
                    
        
        if do_pull_image:
            # The image is not here yet
            pullimage_params = [
                self.docker_cmd,"pull",docker_tag
            ]
            with tempfile.NamedTemporaryFile() as pullimage_stdout:
                with tempfile.NamedTemporaryFile() as pullimage_stderr:
                    retval = subprocess.call(pullimage_params,stdout=pullimage_stdout,stderr=pullimage_stderr)
                    if retval != 0:
                        # Reading the output and error for the report
                        with open(pullimage_stdout.name,"r") as c_stF:
                            pullimage_stdout_v = c_stF.read()
                        with open(pullimage_stderr.name,"r") as c_stF:
                            pullimage_stderr_v = c_stF.read()
                        
                        # It failed!
                        errstr = "ERROR: VRE Nextflow Runner failed while pulling Nextflow image. Tag: {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(docker_tag,pullimage_stdout_v,pullimage_stderr_v)
                        logger.fatal(errstr)
                        raise Exception(errstr)
        
        if configuration is None:
            configuration = {}

        self.configuration.update(configuration)

    def doMaterializeRepo(self, git_uri, git_tag):
        repo_hashed_id = hashlib.sha1(git_uri).hexdigest()
        repo_hashed_tag_id = hashlib.sha1(git_tag).hexdigest()
        
        # Assure directory exists before next step
        repo_destdir = os.path.join(self.wf_basedir,repo_hashed_id)
        if not os.path.exists(repo_destdir):
            try:
                os.makedirs(repo_destdir)
            except IOError as error:
                errstr = "ERROR: Unable to create intermediate directories for repo {}. ".format(git_uri,);
                raise Exception(errstr)
        
        repo_tag_destdir = os.path.join(repo_destdir,repo_hashed_tag_id)
        # We are assuming that, if the directory does exist, it contains the repo
        if not os.path.exists(repo_tag_destdir):
            # Try checking out the repository
            gitclone_params = [
                self.git_cmd,'clone','-b',git_tag,'--recurse-subdirs',git_uri,repo_tag_destdir
            ]
            
            with tempfile.NamedTemporaryFile() as gitclone_stdout:
                with tempfile.NamedTemporaryFile() as gitclone_stderr:
                    retval = subprocess.call(gitclone_params,stdout=gitclone_stdout,stderr=gitclone_stderr)
                    if retval != 0:
                        # Reading the output and error for the report
                        with open(gitclone_stdout.name,"r") as c_stF:
                            gitclone_stdout_v = c_stF.read()
                        with open(gitclone_stderr.name,"r") as c_stF:
                            gitclone_stderr_v = c_stF.read()
                        
                        errstr = "ERROR: VRE Nextflow Runner could not pull '{0}' (tag '{1}')\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(git_uri,git_tag,gitclone_stdout_v,gitclone_stderr_v)
                        raise Exception(errstr)
        
        return repo_tag_destdir

    @task(returns=bool, input_loc=FILE_IN, goldstandard_dir_loc=FILE_IN, assess_dir_loc=FILE_IN, public_ref_dir_loc=FILE_IN, results_loc=FILE_OUT, stats_loc=FILE_OUT, other_loc=FILE_OUT, isModifier=False)
    def validate_and_assess(self, input_loc, goldstandard_dir_loc, assess_dir_loc, public_ref_dir_loc, results_loc, stats_loc, other_loc):  # pylint: disable=no-self-use
        # First, we need to materialize the workflow
        nextflow_repo_uri = self.configuration.get('nextflow_repo_uri')
        nextflow_repo_tag = self.configuration.get('nextflow_repo_tag')
        
        if (nextflow_repo_uri is None) or (nextflow_repo_tag is None):
            logger.fatal("FATAL ERROR: both 'nextflow_repo_uri' and 'nextflow_repo_tag' parameters must be defined")
            return False
        
        # Checking out the repo to be used
        try:
            repo_dir = self.doMaterializeRepo(nextflow_repo_uri,nextflow_repo_tag)
        except Exception as error:
            logger.fatal(str(error))
            return False
        
        event_id = self.configuration['event_id']
        participant_id = self.configuration['participant_id']
        
        inputDir = os.path.dirname(input_loc)
        inputBasename = os.path.basename(input_loc)
        
        # Value needed to compose the Nextflow docker call
        uid = str(os.getuid())
        
        # Should workdir be in a separate place?
        workdir = self.configuration['project']
        
        # Directories required by Nextflow in a Docker
        homedir = os.path.expanduser("~")
        nxf_assets_dir = os.path.join(homedir,".nextflow","assets")
        if not os.path.exists(nxf_assets_dir):
            try:
                os.makedirs(nxf_assets_dir)
            except Exception as error:
                logger.fatal("ERROR: Unable to create nextflow assets directory. Error: "+str(error))
                return False
        
        retval_stage = 'validation'
        
        # The fixed parameters
        validation_cmd_pre_vol = [
            "docker", "run", "--rm", "--net", "host",
            "-e", "USER",
            "-e", "HOME="+homedir,
            "-e", "NXF_ASSETS="+nxf_assets_dir,
            "-e", "NXF_USRMAP="+uid,
            "-e", "NXF_DOCKER_OPTS=-u "+uid+" -e HOME="+homedir,
            "-v", "/var/run/docker.sock:/var/run/docker.sock"
        ]
        
        validation_cmd_post_vol = [
            "-w", workdir,
            self.nxf_image+":"+self.nxf_version,
            "nextflow", "run", repo_dir, "-profile", "docker"
        ]
        
        # This one will be filled in by the volume parameters passed to docker
        #docker_vol_params = []
        
        # This one will be filled in by the volume meta declarations, used
        # to generate the volume parameters
        volumes = [
            (homedir,"ro,Z"),
            (nxf_assets_dir,"Z"),
            (workdir,"Z"),
            (repo_dir,"ro,Z")
        ]
        
        # These are the parameters, including input and output files and directories
        
        # Parameters which are not input or output files are in the configuration
        variable_params = [
        #    ('event_id',event_id),
        #    ('participant_id',participant_id)
        ]
        for conf_key in self.configuration.keys():
            if conf_key not in self.MASKED_KEYS:
                variable_params.append((conf_key,self.configuration[conf_key]))
        
        
        variable_infile_params = [
            ('input',input_loc),
            ('goldstandard_dir',goldstandard_dir_loc),
            ('public_ref_dir',public_ref_dir_loc),
            ('assess_dir',assess_dir_loc)
        ]
        
        variable_outfile_params = [
            ('statsdir',stats_loc),
            ('outdir',results_loc),
            ('otherdir',other_loc)
        ]
        
        # Preparing the RO volumes
        for ro_loc_id,ro_loc_val in variable_infile_params:
            volumes.append((ro_loc_val,"ro,Z"))
            variable_params.append((ro_loc_id,ro_loc_val))
        
        # Preparing the RW volumes
        for rw_loc_id,rw_loc_val in variable_outfile_params:
            volumes.append((rw_loc_val,"Z"))
            variable_params.append((rw_loc_id,rw_loc_val))
        
        # Assembling the command line    
        validation_params = []
        validation_params.extend(validation_cmd_pre_vol)
        
        for volume_dir,volume_mode in volumes:
            validation_params.append("-v")
            validation_params.append(volume_dir+':'+volume_dir+':'+volume_mode)
        
        validation_params.extend(validation_cmd_post_vol)
        
        # Last, but not the least important
        for param_id,param_val in variable_params:
            validation_params.append("--" + param_id)
            validation_params.append(param_val)
        
        #print("DEBUG: "+'  '.join(validation_params),file=sys.stderr)
        retval = subprocess.call(validation_params)
        
        resultsDir = None
        resultsTarDir = None
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
        
        tar_nf_stats_path = output_files.get("tar_nf_stats")
        if tar_nf_stats_path is None:
            tar_nf_stats_path = os.path.join(project_path,'nfstats.tar.gz')
        tar_nf_stats_path = os.path.abspath(tar_nf_stats_path)
        output_files['tar_nf_stats'] = tar_nf_stats_path
        
        tar_other_path = output_files.get("tar_other")
        if tar_other_path is None:
            tar_other_path = os.path.join(project_path,'other_files.tar.gz')
        tar_other_path = os.path.abspath(tar_other_path)
        output_files['tar_other'] = tar_other_path
        
        # Creating the output directories
        results_path = os.path.join(project_path,'results')
        os.makedirs(results_path)
        stats_path = os.path.join(project_path,'nf_stats')
        os.makedirs(stats_path)
        other_path = os.path.join(project_path,'other_files')
        os.makedirs(other_path)
        
        results = self.validate_and_assess(
            os.path.abspath(input_files["input"]),
            os.path.abspath(input_files['goldstandard_dir']),
            os.path.abspath(input_files['assess_dir']),
            os.path.abspath(input_files['public_ref_dir']),
            results_path,
            stats_path,
            other_path
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
                sources=[input_metadata["input"].file_path],
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
            "tar_nf_stats": Metadata(
                # These ones are already known by the platform
                # so comment them by now
                data_type="tool_statistics",
                file_type="TAR",
                file_path=tar_nf_stats_path,
                # Reference and golden data set paths should also be here
                sources=[metrics_path],
                meta_data={
                    "tool": "VRE_NF_RUNNER"
                }
            ),
            "tar_other": Metadata(
                # These ones are already known by the platform
                # so comment them by now
                data_type="tool_statistics",
                file_type="TAR",
                file_path=tar_other_path,
                # Reference and golden data set paths should also be here
                sources=[metrics_path],
                meta_data={
                    "tool": "VRE_NF_RUNNER"
                }
            )
        }

        return (output_files, output_metadata)
