println 'Start'
def result = new File('\\temp\\benchmark.txt')
result.delete()
def files = ['\\temp\\text.dat']
result << "files = $files\n"
for(inputFile in files){
    result << inputFile << "\n" << inputFile.length() << "B\n"
    result << "threads number" << "\t" << "elapsed time" << "\n"
    println inputFile
    for(i in 10..1){
        10.times {
            println "number of threads: $i"
            result << "$i\t"
            def start = System.nanoTime()
            def cmd = "esort.bat $inputFile $i \\temp\\out${i}.dat"
            println cmd
            def proc = cmd.execute()
            proc.consumeProcessOutput()
            proc.waitFor()
            result << "${(System.nanoTime() - start)/1000000}\n"
            println proc.text
        }        
    }
}
println 'Finished'