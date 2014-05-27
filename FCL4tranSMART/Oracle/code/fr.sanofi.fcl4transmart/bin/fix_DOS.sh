#  finds DOS in any transformation and changes it to UNIX
#  This is used when transformations are developed on Windows but are executed on UNIX/Linux systems
#  The Windows line terminators are CRLF, for UNIX/Linux, the termination is only the LF
for f in `find . -name "*.ktr" -type f -exec grep -l 'DOS' {} \;`
do
        sed -i 's/DOS/UNIX/' $f
        echo updated $f
done
