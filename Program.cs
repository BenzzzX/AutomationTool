using Microsoft.Win32;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace Test
{
    enum ETargetPlatform
    {
        Win64
    }

    enum ETargetBuild
    {
        Shipping,
        Development,
        DebugGame,
    }

    class Program
    {
        public static void Swap<T>(ref T lhs, ref T rhs)
        {
            T temp = lhs;
            lhs = rhs;
            rhs = temp;
        }

        public const string checkSumFileName = "csm.json";
        public const string RegistryPath = "SOFTWARE\\Epic Games\\Unreal Engine\\Builds";
        public const string RegistryPath2 = "SOFTWARE\\EpicGames\\Unreal Engine";
        public const string LuaListFileName = "hotupdatelist.lua";

        static void Main(string[] args)
        {
            Debug.WriteLine(string.Join(", ", args));
            if(args.Count() > 0 && string.Compare(args[0], "-run-project", true) == 0)
            {
                var ProjectPath = new DirectoryInfo(args[1]);
                string CmdExePath = GetEngineWin64Path(FindUproject(ProjectPath)).FullName + "\\UnrealEditor-Cmd.exe";
                List<string> CommandTokens = new List<string>();
                CommandTokens.Add(FindUproject(ProjectPath).FullName);
                CommandTokens.AddRange(args.Skip(2));
                Exec(CmdExePath, string.Join(" ", CommandTokens), null);
                return;
            }
            var rootCommand = new RootCommand("Seven Days Tool Set");
            var CopyEngineCommand = new Command("update-engine") {
                new Option<bool> (
                new string[] { "--upload", "-u" },
                "Upload file to server instead of download"
                ),
                new Option<bool> (
                new string[] { "--check", "-c" },
                "Check file before upload or download"
                ),
                new Argument<DirectoryInfo> (
                "Server-Path",
                "Server directory path"
                ),
                new Argument<DirectoryInfo> (
                "Engine-Path",
                "Engine directory path"
                )
            };
            rootCommand.Add(CopyEngineCommand);
            CopyEngineCommand.Handler = CommandHandler.Create<bool, bool, DirectoryInfo, DirectoryInfo>((upload, check, serverPath, enginePath) =>
            {
                var operationName = "engine";
                // if (checksum)
                // {
                //     var mainbar = new ProgressBar(0, "Processing", options);
                //     var filePath = Path.Combine(enginePath.ToString(), $"{operationName}-{checkSumFileName}");
                //     Dictionary<string, FileIndentifier> prevCsm = TryReadDatabase(filePath);
                //     var csm = GenerateDatabase(operationName, enginePath, "*", mainbar, null, prevCsm);
                //     mainbar.Dispose();
                //     File.WriteAllText(Path.Combine(enginePath.ToString(), $"{operationName}-{checkSumFileName}"), JsonConvert.SerializeObject(csm, Formatting.Indented));
                //     return;
                // }

                if (upload)
                    UploadFile(operationName, enginePath, serverPath, "*", null, check);
                else
                {
                    var Editor = System.Diagnostics.Process.GetProcessesByName("UnrealEditor");
                    if (Editor.Length > 0)
                    {
                        Console.WriteLine("Editor is still running, abort!");
                        return;
                    }
                    DownloadFile(operationName, serverPath, enginePath);
                    SetRegistry(enginePath.ToString());
                }

            });

            var UpdateEngineCommand = new Command("update-project-engine")
            {
                new Argument<DirectoryInfo> (
                "Server-Path",
                "Server directory path"
                ),
                new Argument<DirectoryInfo> (
                "Proj-Path",
                "Project directory path"
                ),
                new Option<bool> (
                "--no-plugin",
                "Whether to not update installed plugin"
                )
            };
            rootCommand.Add(UpdateEngineCommand);
            UpdateEngineCommand.Handler = CommandHandler.Create<DirectoryInfo, DirectoryInfo, bool>((serverPath, projPath, noPlugin) =>
            {
                FileInfo Uproject = FindUproject(projPath);
                var enginePath = GetEnginePath(Uproject);

                var Editor = System.Diagnostics.Process.GetProcessesByName("UnrealEditor");
                if (Editor.Length > 0)
                {
                    Console.WriteLine("Editor is still running, abort!");
                    return;
                }

                DownloadFile("engine", serverPath, enginePath);
                if(!noPlugin)
                    DownloadFile("plugin", new DirectoryInfo(Path.Combine(serverPath.ToString(), "Engine/Plugins")), new DirectoryInfo(Path.Combine(enginePath.ToString(), "Engine/Plugins")));
            });

            var CopyPluginCommand = new Command("update-plugin") {
                new Option<bool> (
                new string[] { "--upload", "-u" },
                "Upload file to server instead of download"
                ),
                new Option<bool> (
                new string[] { "--check", "-c" },
                "Check file before upload or download"
                ),
                new Argument<DirectoryInfo> (
                "Server-Path",
                "Server directory path"
                ),
                new Argument<DirectoryInfo> (
                "Proj-Path",
                "Project directory path"
                )
            };
            rootCommand.Add(CopyPluginCommand);
            CopyPluginCommand.Handler = CommandHandler.Create<bool, bool, DirectoryInfo, DirectoryInfo>((upload, check, serverPath, projPath) =>
            {
                var operationName = "plugin";

                if (upload)
                    UploadFile(operationName, projPath, serverPath, "*", null, check);
                else
                {
                    var Editor = System.Diagnostics.Process.GetProcessesByName("UnrealEditor");
                    if (Editor.Length > 0)
                    {
                        Console.WriteLine("Editor is still running, abort!");
                        return;
                    }
                    DownloadFile(operationName, serverPath, projPath);
                }
            });

            var CopyPBDCommand = new Command("update-pdb") {
                new Option<bool> (
                new string[] { "--upload", "-u" },
                "Upload file to server instead of download"
                ),
                new Option<bool> (
                new string[] { "--check", "-c" },
                "Check file before upload or download"
                ),
                new Argument<DirectoryInfo> (
                "Server-Path",
                "Server directory path"
                ),
                new Argument<DirectoryInfo> (
                "Proj-Path",
                "Project directory path"
                )
            };
            rootCommand.Add(CopyPBDCommand);
            CopyPBDCommand.Handler = CommandHandler.Create<bool, bool, DirectoryInfo, DirectoryInfo>((upload, check, serverPath, projPath) =>
            {
                var operationName = "pdb";
                Console.WriteLine($"Copying pdb from {serverPath} to {projPath}");
                string pattern = @".*-\d\d\d\d\.pdb";
                if (upload)
                    UploadFile(operationName, projPath, serverPath, "*.pdb", x => !x.Contains("DebugGame") && !Regex.IsMatch(x, pattern) && x.Contains("Binaries"), check);
                else
                {
                    DownloadFile(operationName, serverPath, projPath);
                    ClearFile(operationName, projPath, "*.pdb", x => !x.Contains("DebugGame") && !Regex.IsMatch(x, pattern) && x.Contains("Binaries"));
                }
            });

            var InstallEngineCommand = new Command("install-engine") {
                new Argument<DirectoryInfo> (
                "Engine-Path",
                "Path of Engine to install"
                ),
            };
            rootCommand.Add(InstallEngineCommand);
            InstallEngineCommand.Handler = CommandHandler.Create<DirectoryInfo>((enginePath) =>
            {
                SetRegistry(enginePath.ToString());
            });

            var BuildPluginCommand = new Command("build-plugin") {
                new Argument<DirectoryInfo> (
                "Project-Path",
                "Path of Plugins"
                ),
                new Argument<DirectoryInfo> (
                "Plugin-Path",
                "Path of Plugins"
                ),
                new Argument<DirectoryInfo> (
                "Output-Path",
                "Path of builded output"
                ),
                new Option<string[]>(
                "--plugins",
                "Name of plugin to build"
                ),
                new Option<string>(
                "--engine",
                "Name of engine to use"
                )
            };
            rootCommand.Add(BuildPluginCommand);
            BuildPluginCommand.Handler = CommandHandler.Create<DirectoryInfo, DirectoryInfo, DirectoryInfo, IEnumerable<string>, string>((ProjectPath, pluginPath, outputPath, plugins, engine) =>
            {
                string EnginePath = null;
                if(engine != null)
                {
                    EnginePath = ReadEnginePathRegistry(engine).FullName;
                }
                else
                {
                    EnginePath = GetEnginePath(FindUproject(ProjectPath)).FullName;
                }
                 
                if (EnginePath == null)
                {
                    Console.WriteLine("Engine not found, abort!");
                    return;
                }
                var UATPath = Path.Combine(EnginePath, "Engine/Build/BatchFiles/RunUAT.bat");
                bool appendLog = false;;
                Directory.GetFiles(pluginPath.ToString(), "*.uplugin", SearchOption.AllDirectories)
                    .ToList().ForEach(x =>
                    {
                        if(plugins != null && !plugins.Contains(Path.GetFileNameWithoutExtension(x)))
                            return;
                        var RelativePath = Path.GetRelativePath(pluginPath.ToString(), Path.GetDirectoryName(x));
                        var BuildPath = Path.Combine(outputPath.ToString(), RelativePath);
                        Directory.CreateDirectory(BuildPath);
                        var CommandLine = $"BuildPlugin -VS2019 -Plugin=\"{x}\" -Package=\"{BuildPath}\" -CreateSubFolder -TargetPlatforms=Win64";
                        
                        Exec(UATPath, CommandLine, new FileInfo(Path.Combine(pluginPath.ToString(), "Logs", "Build.log")), appendLog);
                        appendLog = true;
                        Console.WriteLine("\n");
                    });
            });

            var UpdateCommand = new Command("update") {
                new Argument<DirectoryInfo> (
                "Server-Path",
                "Path of update source"
                ),
            };
            rootCommand.Add(UpdateCommand);
            UpdateCommand.Handler = CommandHandler.Create<DirectoryInfo>((serverPath) =>
            {
                var name = "PATH";
                var scope = EnvironmentVariableTarget.Machine; // or User
                var oldValue = Environment.GetEnvironmentVariable(name, scope);
                var newValue = oldValue + @";C:\Program Files\MySQL\MySQL Server 5.1\bin\\";
                Environment.SetEnvironmentVariable(name, newValue, scope);
            });

            var UpdateLuaListCommand = new Command("update-lua") {
                new Argument<DirectoryInfo> (
                "Root-Directory",
                "Lua script root directory"
                ),
                new Option<string[]> (
                "--black-list",
                "Lua file name to ignore"
                )
            };
            rootCommand.Add(UpdateLuaListCommand);
            UpdateLuaListCommand.Handler = CommandHandler.Create<DirectoryInfo, IEnumerable<string>>((rootDirectory, blacklist) =>
            {
                var LuaList = rootDirectory.EnumerateFiles("*.lua", SearchOption.AllDirectories).Where(x => !blacklist.Contains(x.Name)).Aggregate("", (str, File) => str + $"  \"{Path.GetFileNameWithoutExtension(File.Name)}\",\n");
                var FileContent = $"local FileNameList = {{\n{LuaList}}} \nreturn FileNameList";
                File.WriteAllText(Path.Combine(rootDirectory.ToString(), LuaListFileName), FileContent);
            });

            var FillDDCCommand = new Command("fill-ddc") {
                new Argument<DirectoryInfo> (
                "Project-Path",
                "项目根目录"
                )
            };
            rootCommand.Add(FillDDCCommand);
            FillDDCCommand.Handler = CommandHandler.Create<DirectoryInfo>((ProjectPath) =>
            {
                string CmdExePath = GetEngineWin64Path(FindUproject(ProjectPath)).FullName + "\\UnrealEditor-Cmd.exe";

                Exec(CmdExePath, FindUproject(ProjectPath).FullName + " -run=DerivedDataCache -fill -MAPSONLY", null);
            });

            var BatchCompileBlueprintsCommand = new Command("batch-compile-blueprints") {
                new Argument<DirectoryInfo> (
                "Project-Path",
                "项目根目录"
                )
            };
            rootCommand.Add(BatchCompileBlueprintsCommand);
            BatchCompileBlueprintsCommand.Handler = CommandHandler.Create<DirectoryInfo>((ProjectPath) =>
            {
                string CmdExePath = GetEngineWin64Path(FindUproject(ProjectPath)).FullName + "\\UnrealEditor-Cmd.exe";

                Exec(CmdExePath, FindUproject(ProjectPath).FullName + " -run=CompileAllBlueprints -utf8output", null);
            });


            var BuildGameCommand = new Command("build-game") {
                new Argument<DirectoryInfo> (
                "Project-Path",
                "项目根目录"
                ),
                new Argument<ETargetPlatform> (
                "Target-Platform",
                "目标平台"
                ),
                new Argument<ETargetBuild> (
                "Target-Build",
                "游戏打包目标"
                ),
                new Argument<DirectoryInfo> (
                "Out",
                "输出目录"
                )
            };
            rootCommand.Add(BuildGameCommand);
            BuildGameCommand.Handler = CommandHandler.Create<DirectoryInfo, ETargetPlatform, ETargetBuild, DirectoryInfo>((ProjectPath, TargetPlatform, TargetBuild, Out) =>
            {
                FileInfo Uproject = FindUproject(ProjectPath);
                string ProjectName = Path.GetFileNameWithoutExtension(Uproject.ToString());
                string CmdExePath = "\"" + GetEngineWin64Path(FindUproject(ProjectPath)).FullName + "\\UnrealEditor-Cmd.exe" + "\"";
                string UnrealBuildToolPath = GetEngineWin64Path(Uproject).FullName + "\\UnrealBuildTool.exe";
                string UATPath = GetEngineBatchFilesPath(Uproject).FullName + "\\RunUAT.bat";

                string Arguments;

                Console.WriteLine("打包游戏\n");
                Arguments = String.Format("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16} {17}",
                    "-ScriptsForProject=" + Uproject.FullName,
                    "BuildCookRun",
                    "-installed",
                    "-nop4",
                    "-project=" + Uproject.FullName,
                    "-cook",
                    "-stage",
                    "-archive -archivedirectory=" + Out.FullName,
                    "-package",
                    "-ueexe=" + CmdExePath,
                    "-compressed",
                    "-pak",
                    "-prereqs",
                    "-targetplatform=" + TargetPlatform.ToString(),
                    "-build",
                    "-target=" + ProjectName,
                    "-clientconfig=" + TargetBuild.ToString(),
                    "-utf8output"
                );
                Exec(UATPath, Arguments, new FileInfo(Path.Join(Out.FullName, "PackagingGame.log")));
            });

            rootCommand.InvokeAsync(args);
        }

        static void Exec(string exePath, string parameters, FileInfo OutLog = null, bool appendLog = false)
        {
            Console.WriteLine("启动 "+ exePath + " " + parameters);
            ProcessStartInfo psi =
            new ProcessStartInfo();
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
            psi.WindowStyle = ProcessWindowStyle.Hidden;
            psi.UseShellExecute = false;
            psi.FileName = exePath;
            psi.Arguments = parameters;

            StreamWriter file = null;
            if (OutLog != null)
            {
                if (!OutLog.Directory.Exists) OutLog.Directory.Create();
                file = new StreamWriter(OutLog.FullName, appendLog);
            }

            using (var process = new System.Diagnostics.Process())
            {
                process.StartInfo = psi;
                process.EnableRaisingEvents = true;

                process.OutputDataReceived += (s, o) => {
                    Console.WriteLine(o.Data);
                    if (file != null) file.WriteLine(o.Data);
                };
                process.Start();

                process.BeginOutputReadLine();

                process.WaitForExit();
            }

            if (file != null)
            {
                file.Flush();
                file.Close();
            }
        }

        static FileInfo FindUproject(DirectoryInfo ProjectPath)
        {
            var Files = ProjectPath.GetFiles("*.uproject");
            if (Files.Length == 1)
            {
                return Files[0];
            }
            else if (Files.Length > 1)
                throw new System.ArgumentException("找到多个 *.uproject");

            throw new System.ArgumentException("找不到 *.uproject");
        }

        static DirectoryInfo ReadEnginePathRegistry(string EngineAssociation)
        {
            string EnginePath = "";
            using (var Key = Registry.CurrentUser.OpenSubKey(RegistryPath, true))
            {
                if (Key != null)
                {
                    if (Key.GetValueNames().Contains(EngineAssociation) && Key.GetValueKind(EngineAssociation) == RegistryValueKind.String)
                        EnginePath = (string)Key.GetValue(EngineAssociation);
                }
            }
            using (var Key = Registry.LocalMachine.OpenSubKey(RegistryPath2 + "\\" + EngineAssociation, true))
            {
                if (Key != null)
                {
                    if (Key.GetValueKind("InstalledDirectory") == RegistryValueKind.String)
                        EnginePath = (string)Key.GetValue("InstalledDirectory");
                }
            }
            if (EnginePath == "") throw new System.ArgumentException("找不到引擎路径");
            return new DirectoryInfo(EnginePath);
        }

        static DirectoryInfo GetEnginePath(FileInfo ProjectPath)
        {
            using (System.IO.StreamReader file = System.IO.File.OpenText(ProjectPath.FullName))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    JObject o = (JObject)JToken.ReadFrom(reader);
                    var EngineAssociation = o["EngineAssociation"].ToString();

                    return ReadEnginePathRegistry(EngineAssociation);
                }
            }
            throw new System.ArgumentException("找不到引擎版本信息");
        }

        static DirectoryInfo GetEngineWin64Path(FileInfo ProjectPath)
        {
            return new DirectoryInfo(Path.Join(GetEnginePath(ProjectPath).ToString(), "Engine\\Binaries\\Win64"));
        }

        static DirectoryInfo GetEngineDotNETPath(FileInfo ProjectPath)
        {
            return new DirectoryInfo(Path.Join(GetEnginePath(ProjectPath).ToString(), "Engine\\Binaries\\DotNET"));
        }

        static DirectoryInfo GetEngineBatchFilesPath(FileInfo ProjectPath)
        {
            return new DirectoryInfo(Path.Join(GetEnginePath(ProjectPath).ToString(), "Engine\\Build\\BatchFiles"));
        }

        static void SetRegistry(string path)
        {
            RegistryKey Key = Registry.CurrentUser.OpenSubKey(RegistryPath, true);
            if (Key == null)
            {
                Key = Registry.CurrentUser.CreateSubKey(RegistryPath, true);
            }
            if (Key == null)
            {
                Console.WriteLine("Fail to access registry!");
            }
            Key.SetValue("RealEngine", path);
            Key.Dispose();
        }

        [Serializable]
        class FileIndentifier
        {
            public string MD5;
            public long LastWriteTime;
            public long Length;
        }

        static ProgressBarOptions options = new ProgressBarOptions
        {
            ForegroundColor = ConsoleColor.Yellow,
            BackgroundColor = ConsoleColor.DarkYellow,
        };
        static ProgressBarOptions childOptions = new ProgressBarOptions
        {
            ProgressCharacter = '─',
            CollapseWhenFinished = false
        };

        static string CalculateMD5(string filename)
        {
            using (var md5 = MD5.Create())
            {
                using (var stream = File.OpenRead(filename))
                {
                    var hash = md5.ComputeHash(stream);
                    return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
                }
            }
        }

        static void DownloadFile(string OperationName, DirectoryInfo srcPath, DirectoryInfo dstPath)
        {
            var srcFilePath = Path.Combine(srcPath.ToString(), $"{OperationName}-{checkSumFileName}");
            var dstFilePath = Path.Combine(dstPath.ToString(), $"{OperationName}-{checkSumFileName}");
            dstPath.Create();
            Dictionary<string, FileIndentifier> srcScm = TryReadDatabase(srcFilePath);
            Dictionary<string, FileIndentifier> dstScm = TryReadDatabase(dstFilePath);

            var mainbar = new ProgressBar(3, "Fetching", options);
            if (dstScm == null)
                dstScm = new Dictionary<string, FileIndentifier>();
            mainbar.Tick("Downloading");
            List<string> FileToCopy = new List<string>();
            foreach (var pair in srcScm)
            {
                if (dstScm.TryGetValue(pair.Key, out FileIndentifier value))
                    if (value.MD5 == pair.Value.MD5)
                        continue;
                FileToCopy.Add(pair.Key);
            }
            try
            {
                var pbar = mainbar.Spawn(FileToCopy.Count, "Processing", childOptions);
                foreach (var fp in FileToCopy)
                {
                    pbar.Tick($"Processing {fp}");
                    var sp = Path.Combine(srcPath.ToString(), fp);
                    var dp = Path.Combine(dstPath.ToString(), fp);
                    Directory.CreateDirectory(Path.GetDirectoryName(dp));
                    File.Copy(sp, dp, true);
                    File.SetLastWriteTimeUtc(dp, DateTime.FromBinary(srcScm[fp].LastWriteTime));
                    dstScm[fp] = srcScm[fp];
                }
                pbar.Dispose();

                mainbar.Tick("Cleaning");
                List<string> FileToDelete = new List<string>();
                foreach (var pair in dstScm)
                {
                    if (!srcScm.TryGetValue(pair.Key, out FileIndentifier value))
                        FileToDelete.Add(pair.Key);
                }
                pbar = mainbar.Spawn(FileToDelete.Count, "Processing", childOptions);
                foreach (var fp in FileToDelete)
                {
                    pbar.Tick($"Processing {fp}");
                    var dp = Path.Combine(dstPath.ToString(), fp);
                    if (File.Exists(dp))
                        File.Delete(dp);
                    dstScm.Remove(fp);
                }
                pbar.Dispose();
                mainbar.Tick("Validating");
                pbar = mainbar.Spawn(dstScm.Count, "Processing", childOptions);
                var UpdatedInfo = new List<(string, FileIndentifier)>();
                foreach (var pair in dstScm)
                {
                    pbar.Tick($"Processing {pair.Key}");
                    if (!srcScm.TryGetValue(pair.Key, out FileIndentifier value))
                        continue;
                    var sp = Path.Combine(srcPath.ToString(), pair.Key);
                    var dp = new FileInfo(Path.Combine(dstPath.ToString(), pair.Key));
                    if (!dp.Exists)
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(dp.ToString()));
                        File.Copy(sp, dp.ToString(), true);
                        File.SetLastWriteTimeUtc(dp.ToString(), DateTime.FromBinary(value.LastWriteTime));
                        continue;
                    }
                    if (value.LastWriteTime == dp.LastWriteTimeUtc.ToBinary() && value.Length == dp.Length)
                        continue;
                    var dstMD5 = CalculateMD5(dp.ToString());
                    if (dstMD5 == value.MD5)
                        continue;
                    Directory.CreateDirectory(Path.GetDirectoryName(dp.ToString()));
                    File.Copy(sp, dp.ToString(), true);
                    File.SetLastWriteTimeUtc(dp.ToString(), DateTime.FromBinary(value.LastWriteTime));
                    UpdatedInfo.Add((pair.Key, value));
                }
                foreach(var (key, value) in UpdatedInfo)
                    dstScm[key] = value;

                pbar.Dispose();
                mainbar.Tick("Finished");
                mainbar.Dispose();
            }
            finally
            {
                var dstCsmFilePath = Path.Combine(dstPath.ToString(), $"{OperationName}-{checkSumFileName}");
                File.WriteAllText(dstCsmFilePath, JsonConvert.SerializeObject(dstScm, Formatting.Indented));
            }
        }

        static void ClearFile(string operationName, DirectoryInfo path, string filter, Func<string, bool> predicate)
        {
            predicate = predicate ?? (x => true);
            var csmFilePath = Path.Combine(path.ToString(), $"{operationName}-{checkSumFileName}");
            Dictionary<string, FileIndentifier> csm = null;

            if (File.Exists(csmFilePath))
            {
                var csmString = File.ReadAllText(csmFilePath);
                try
                {
                    csm = JsonConvert.DeserializeObject<Dictionary<string, FileIndentifier>>(csmString);
                }
                catch (System.Exception e)
                {
                    Console.WriteLine($"Fail to parse csm file. {e.ToString()}");
                }
            }
            else
            {
                csm = new Dictionary<string, FileIndentifier>();
                File.Create(csmFilePath).Close();
            }
            var FileList = Directory.GetFiles(path.ToString(), filter).ToList();
            FileList.Where(predicate).ToList().ForEach(x =>
            {
                if (x == csmFilePath)
                    return;
                if (!csm.TryGetValue(x.ToString(), out var _))
                    File.Delete(x);
            });
        }

        static Dictionary<string, FileIndentifier> GenerateDatabase(string operationName, DirectoryInfo path, string filter, ProgressBarBase pbar, Func<string, bool> predicate = null, Dictionary<string, FileIndentifier> reference = null)
        {
            Dictionary<string, FileIndentifier> csm = new Dictionary<string, FileIndentifier>();
            var csmFilePath = Path.Combine(path.ToString(), $"{operationName}-{checkSumFileName}");
            predicate = predicate ?? (x => true);
            var options = new ProgressBarOptions { };
            var FileList = Directory.GetFiles(path.ToString(), filter, SearchOption.AllDirectories).Where(predicate).ToList();
            pbar.MaxTicks = FileList.Count;
            FileList.ForEach(x =>
            {
                if (x.Contains("-csm") || x.Contains(".aborted"))
                    return;
                var RelativePath = Path.GetRelativePath(path.ToString(), x);
                pbar.Tick($"Processing {RelativePath}");
                var Info = new FileInfo(x);
                if (reference != null && reference.TryGetValue(RelativePath, out FileIndentifier value))
                    if (value.LastWriteTime == Info.LastWriteTimeUtc.ToBinary() && value.Length == Info.Length)
                    {
                        csm[RelativePath] = value;
                        return;
                    }
                csm[RelativePath] = new FileIndentifier
                {
                    MD5 = CalculateMD5(x),
                    LastWriteTime = Info.LastWriteTimeUtc.ToBinary(),
                    Length = Info.Length
                };
            });
            return csm;
        }

        static Dictionary<string, FileIndentifier> TryReadDatabase(string csmFilePath)
        {
            Dictionary<string, FileIndentifier> csm = null;
            if (File.Exists(csmFilePath))
            {
                var csmString = File.ReadAllText(csmFilePath);
                try
                {
                    csm = JsonConvert.DeserializeObject<Dictionary<string, FileIndentifier>>(csmString);
                }
                catch (System.Exception e)
                {
                    Console.WriteLine($"Fail to parse csm file. {e.ToString()}");
                }
            }
            return csm;
        }

        static void UploadFile(string operationName, DirectoryInfo srcPath, DirectoryInfo dstPath, string filter, Func<string, bool> predicate, bool performCheck)
        {
            var srcCsmFilePath = Path.Combine(srcPath.ToString(), $"{operationName}-{checkSumFileName}");
            var dstCsmFilePath = Path.Combine(dstPath.ToString(), $"{operationName}-{checkSumFileName}");
            var abn = Path.Combine(srcPath.ToString(), ".aborted", dstCsmFilePath.ToString().Replace("\\", "-").Replace("/", "-"));

            var mainbar = new ProgressBar(performCheck ? 4 : 5, "Fetching", options);
            Dictionary<string, FileIndentifier> dstCsm = null;
            mainbar.Tick("Analysing");
            Dictionary<string, FileIndentifier> srcCsm = TryReadDatabase(srcCsmFilePath);
            if(File.Exists(abn) && File.Exists(dstCsmFilePath))
            {
                if(File.GetLastWriteTimeUtc(abn) > File.GetLastWriteTimeUtc(dstCsmFilePath))
                    dstCsm = TryReadDatabase(abn);
                if(dstCsm == null)
                    dstCsm = TryReadDatabase(dstCsmFilePath);
            }
            if(dstCsm == null)
                dstCsm = TryReadDatabase(abn);
            if(dstCsm == null)
                dstCsm = TryReadDatabase(dstCsmFilePath);
                
            {
                var pbar = mainbar.Spawn(0, "Processing", childOptions);
                srcCsm = GenerateDatabase(operationName, srcPath, filter, pbar, predicate, srcCsm);
                pbar.Dispose();
            }

            if(performCheck)
            {
                mainbar.Tick("Checking");
                var pbar = mainbar.Spawn(0, "Checking", childOptions);
                dstCsm = GenerateDatabase(operationName, dstPath, filter, pbar, predicate, dstCsm);
                File.WriteAllText(dstCsmFilePath, JsonConvert.SerializeObject(dstCsm, Formatting.Indented));
                pbar.Dispose();
            }
            

            if (dstCsm == null)
                dstCsm = new Dictionary<string, FileIndentifier>();

            mainbar.Tick("Uploading");
            List<string> FileToCopy = new List<string>();
            foreach (var pair in srcCsm)
            {
                if (dstCsm.TryGetValue(pair.Key, out FileIndentifier value))
                    if (value.MD5 == pair.Value.MD5)
                        continue;
                FileToCopy.Add(pair.Key);
            }
            List<string> FileToDelete = new List<string>();
            foreach (var pair in dstCsm)
            {
                if (!srcCsm.TryGetValue(pair.Key, out FileIndentifier value))
                    FileToDelete.Add(pair.Key);
            }
            try
            {
                var pbar = mainbar.Spawn(FileToCopy.Count, "Processing", childOptions);
                foreach (var fp in FileToCopy)
                {
                    pbar.Tick($"Processing {fp}");
                    var sp = Path.Combine(srcPath.ToString(), fp);
                    var dp = Path.Combine(dstPath.ToString(), fp);
                    Directory.CreateDirectory(Path.GetDirectoryName(dp));
                    File.Copy(sp, dp, true);
                    dstCsm[fp] = srcCsm[fp];
                }
                pbar.Dispose();
                mainbar.Tick("Cleaning");
                pbar = mainbar.Spawn(FileToDelete.Count, "Processing", childOptions);
                foreach (var fp in FileToDelete)
                {
                    pbar.Tick($"Processing {fp}");
                    var dp = Path.Combine(dstPath.ToString(), fp);
                    File.Delete(dp);
                    dstCsm.Remove(fp);
                }
                pbar.Dispose();
                mainbar.Tick("Finished");
                mainbar.Dispose();
            }
            catch(Exception)
            {
                //Console.WriteLine(e.ToString());
                Directory.CreateDirectory(Path.GetDirectoryName(abn));
                File.WriteAllText(abn, JsonConvert.SerializeObject(dstCsm, Formatting.Indented));
                throw;
            }
            finally
            {
                File.WriteAllText(srcCsmFilePath, JsonConvert.SerializeObject(srcCsm, Formatting.Indented));
            }
            File.WriteAllText(dstCsmFilePath, JsonConvert.SerializeObject(dstCsm, Formatting.Indented));
            if(File.Exists(abn))
                File.Delete(abn);
        }
    }
}