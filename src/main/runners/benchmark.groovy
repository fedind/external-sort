println 'Start'
def result = new File('\\temp\\benchmark.txt')
result.delete()
def files = ['\\temp\\text.dat']
result << "files = $files\n"
for(inputFile in files){
    result << inputFile << "\n" << inputFile.length() / 1024*1024 << "MB\n"
    println inputFile
    for(i in 10..1){
        println "number of threads: $i"
        result << "number of threads: $i\n"
        def start = System.nanoTime()
        def cmd = """esort.bat $inputFile $i \\temp\\out${i}.dat"""
        println cmd
        result << cmd << "\n"
        def proc = cmd.execute()
        proc.waitFor()
        result << "File: $inputFile\n" << "Threads number: $i\n" << "Elapsed time: ${(System.nanoTime() - start)/1000000}ms\n\n"
    }
}
println 'Finished'