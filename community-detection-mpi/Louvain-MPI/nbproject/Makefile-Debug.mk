#
# Generated Makefile - do not edit!
#
# Edit the Makefile in the project folder instead (../Makefile). Each target
# has a -pre and a -post target defined where you can add customized code.
#
# This makefile implements configuration specific macros and targets.


# Environment
MKDIR=mkdir
CP=cp
GREP=grep
NM=nm
CCADMIN=CCadmin
RANLIB=ranlib
CC=mpicc
CCC=mpic++
CXX=mpic++
FC=gfortran
AS=as

# Macros
CND_PLATFORM=GNU_MPI-Linux-x86
CND_DLIB_EXT=so
CND_CONF=Debug
CND_DISTDIR=dist
CND_BUILDDIR=build

# Include project Makefile
include Makefile

# Object Directory
OBJECTDIR=${CND_BUILDDIR}/${CND_CONF}/${CND_PLATFORM}

# Object Files
OBJECTFILES= \
	${OBJECTDIR}/_ext/1204994903/community.o \
	${OBJECTDIR}/_ext/1204994903/graph.o \
	${OBJECTDIR}/_ext/1204994903/graph_binary.o \
	${OBJECTDIR}/_ext/1204994903/main_hierarchy.o \
	${OBJECTDIR}/community-main.o \
	${OBJECTDIR}/converter.o


# C Compiler Flags
CFLAGS=

# CC Compiler Flags
CCFLAGS=
CXXFLAGS=

# Fortran Compiler Flags
FFLAGS=

# Assembler Flags
ASFLAGS=

# Link Libraries and Options
LDLIBSOPTIONS=

# Build Targets
.build-conf: ${BUILD_SUBPROJECTS}
	"${MAKE}"  -f nbproject/Makefile-${CND_CONF}.mk ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/louvain-mpi

${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/louvain-mpi: ${OBJECTFILES}
	${MKDIR} -p ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}
	${LINK.cc} -o ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/louvain-mpi ${OBJECTFILES} ${LDLIBSOPTIONS}

${OBJECTDIR}/_ext/1204994903/community.o: /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/community.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/1204994903
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/1204994903/community.o /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/community.cpp

${OBJECTDIR}/_ext/1204994903/graph.o: /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/graph.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/1204994903
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/1204994903/graph.o /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/graph.cpp

${OBJECTDIR}/_ext/1204994903/graph_binary.o: /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/graph_binary.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/1204994903
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/1204994903/graph_binary.o /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/graph_binary.cpp

${OBJECTDIR}/_ext/1204994903/main_hierarchy.o: /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/main_hierarchy.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/1204994903
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/1204994903/main_hierarchy.o /media/sf_E_DRIVE/phd-svn/goffish/community-detection-mpi/Louvain-MPI/main_hierarchy.cpp

${OBJECTDIR}/community-main.o: community-main.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/community-main.o community-main.cpp

${OBJECTDIR}/converter.o: converter.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/converter.o converter.cpp

# Subprojects
.build-subprojects:

# Clean Targets
.clean-conf: ${CLEAN_SUBPROJECTS}
	${RM} -r ${CND_BUILDDIR}/${CND_CONF}
	${RM} ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/louvain-mpi

# Subprojects
.clean-subprojects:

# Enable dependency checking
.dep.inc: .depcheck-impl

include .dep.inc
