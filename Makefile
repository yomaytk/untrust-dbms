Cpp_Files := bufferManager.cpp disk.cpp input.cpp main.cpp parser.cpp query.cpp run.cpp util.cpp
Object_Files := bufferManager.o disk.o main.o parser.o query.o run.o util.o
CXX_Flags := -std=c++23
Execution_File := app

all: $(Object_Files)
	@$(CXX) $(Object_Files) -o $(Execution_File)

%.o: %.cpp  c_user_types.h
	@$(CXX) $(CXX_Flags) -fPIC -c $< -o $@

clean: 
	@rm -f $(Object_Files) $(Execution_File)