FROM mcr.microsoft.com/dotnet/core/runtime:3.0-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.0-buster AS build
WORKDIR /src
COPY ["Userservice/Userservice.csproj", "Userservice/"]
RUN dotnet restore "Userservice/Userservice.csproj"
COPY . .
WORKDIR "/src/Userservice"
RUN dotnet build "Userservice.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Userservice.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Userservice.dll"]
