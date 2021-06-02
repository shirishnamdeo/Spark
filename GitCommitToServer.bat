::This is comment


@ECHO OFF
::This above annotation is required, otherwise everything is printerd with the prompt and path on terminal.

::Creeating Alias
::https://stackoverflow.com/questions/20530996/aliases-in-windows-command-prompt
rem https://superuser.com/questions/560519/how-to-set-an-alias-in-windows-command-line
rem DOSKEY command can be used to define macros which can be used as ALIAS
rem doskey macroName=macroDefinition

DOSKEY ls=dir


ECHO Locations
ECHO 1. Git Add 
ECHO 2. Git Add Commit
ECHO 3. Git Add Commit Push
ECHO 4. Git Pull
ECHO 5. Git Status
ECHO 6. Git Log


SET /P choice=Choose Location:

IF %choice% == 1 GOTO :OPTION1
IF %choice% == 2 GOTO :OPTION2
IF %choice% == 3 GOTO :OPTION3
IF %choice% == 4 GOTO :OPTION4
IF %choice% == 5 GOTO :OPTION5
IF %choice% == 6 GOTO :OPTION6
GOTO :END


:OPTION1
	git add .
	GOTO :END
:OPTION2
    git add .
	git commit -m "Update"
	GOTO :END
:OPTION3
	git add .
	git commit -m "Update"
	git push origin master
	GOTO :END
:OPTION4
	git pull origin master
	GOTO :END
:OPTION5
	git status
	GOTO :END
:OPTION6
	git log
	GOTO :END
:END

