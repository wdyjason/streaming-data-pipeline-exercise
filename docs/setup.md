# 流式处理管道课前准备

在本次练习中，需要使用Java、Airflow、PostgreSQL、Docker等完成练习。因此需要学员提前配置好开发环境。

主要包括：
* Java环境：Java 11。
* Java IDE：IntelliJ Idea。
* Docker 和 docker-compose。
* PostgreSQL 的 GUI 客户端。比如 Visual Studio Code 就有相应插件。
* 拉取练习所需的docker镜像。

## Java 11

不同项目可能会使用不同版本的Java，所以建议使用一些版本管理工具，管理本地的Java。比如 [asdf](https://asdf-vm.com/)。

asdf 举例：
* 按照[说明](https://asdf-vm.com/#/core-manage-asdf)安装 asdf
* 安装 Java 插件 `asdf plugin add java`
* 查看可用的 Java 版本 `asdf list all java`
* 安装特定版本的 Python，比如 `asdf install python adoptopenjdk-11.0.11+9`
* 启用该版本 `asdf global java adoptopenjdk-11.0.11+9`

## IntelliJ Idea

可使用 Homebrew 安装

```
brew install intellij-idea
```

## Docker 和 docker-compose

可以从[官网](https://docs.docker.com/docker-for-mac/install/)下载安装，或者使用 Homebrew 安装。

```
brew install docker
```

然后从 Launchpad 里边点击 Docker 图标打开。在 Docker 里的 Preferences 里调整内存大小。建议 6GB 以上。

安装之后终端里即有`docker`和`docker-compose`命令。

## PostgreSQL GUI 客户端

有一些开源的客户端，可自行搜索。但如果使用 Visual Studio Code，那么也可以安装插件来使用，比如 SQL Tool。或者使用你的常用工具。

## 拉取镜像

克隆本代码库，并执行 `docker-compose pull` 拉取镜像

```
git clone https://github.com/data-community/streaming-data-pipeline-exercise
cd streaming-data-pipeline-exercise
docker-compose pull
```

## 安装依赖包

在项目目录执行

```
./gradlew clean build
```
